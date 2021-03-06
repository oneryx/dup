package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	_        = iota // ignore first value by assigning to blank identifier
	KB int64 = 1 << (10 * iota)
	MB
	GB
	TB
)

// file larger than this size will be considered as large file, will hash by samples instead of whole file
const samplethreshold int64 = 3 * MB

// sample piece size
const samplesize int64 = 4 * KB

const empty = ""

// the base dir under which to look for duplicated files
var basedir string

func main() {
	var err error
	var dups []FileGroup
	if len(os.Args) > 1 {
		basedir = os.Args[1]
	} else {
		if basedir, err = os.Getwd(); err != nil {
			log.Fatal(err)
		}
	}
	if dups, err = findDup(basedir); err != nil {
		log.Fatal(err)
	}
	for i, dg := range dups {
		fmt.Printf("%d: %v", i, dg)
	}
}

// FileDetail struct to hold file detail info
type FileDetail struct {
	path string
	size int64
	hash string
}

// FileGroup strct to hold duplicated files together
type FileGroup struct {
	size  string
	hash  string
	files []FileDetail
}

// override String() method to print custom format
func (fg FileGroup) String() string {
	b := strings.Builder{}
	b.WriteString("<Size: ")
	b.WriteString(fg.size)
	b.WriteString(" Bytes, SHA256: ")
	b.WriteString(fg.hash)
	b.WriteString(", Duplication: ")
	b.WriteString(strconv.Itoa(len(fg.files)))
	b.WriteString(">\n")
	for _, f := range fg.files {
		b.WriteString("  ")
		b.WriteString(f.path)
		b.WriteString("\n")
	}
	b.WriteString("\n")
	return b.String()
}

// find duplicated files under dir
func findDup(dir string) ([]FileGroup, error) {
	fmt.Println("Looking for duplicated files under", dir)
	var err error
	var quickHashMap map[string][]FileDetail
	var hashMap map[string][]FileDetail
	var fds = []FileDetail{}
	var dups = []FileGroup{}
	if err = recursiveReadDir(basedir, &fds); err != nil {
		return nil, err
	}

	sizeMap := filterBySize(&fds)

	if quickHashMap, err = filterByHash(sizeMap, true); err != nil {
		return nil, err
	}

	if hashMap, err = filterByHash(quickHashMap, false); err != nil {
		return nil, err
	}

	for k, v := range hashMap {
		s := strings.Split(k, "-")
		dups = append(dups, FileGroup{size: s[0], hash: s[1], files: v})
	}
	if len(dups) == 0 {
		fmt.Println("No duplication found!")
	}
	return dups, nil
}

// file size as map key, to remove files with unique size
func filterBySize(fds *[]FileDetail) map[string][]FileDetail {
	result := make(map[string][]FileDetail)
	for _, f := range *fds {
		key := strconv.FormatInt(f.size, 10)
		g, ok := result[key]
		if ok {
			result[key] = append(g, f)
		} else {
			result[key] = []FileDetail{f}
		}
	}
	for k, v := range result {
		if len(v) <= 1 {
			delete(result, k)
		}
	}
	return result
}

// file size+hash as map key, to remove files with unique hash
func filterByHash(sizeMap map[string][]FileDetail, quick bool) (map[string][]FileDetail, error) {
	var hashstr string
	var key string
	var err error
	result := make(map[string][]FileDetail)
	for _, v := range sizeMap {
		for _, f := range v {
			if hashstr, err = hash(&f, quick); err != nil {
				return nil, err
			}
			key = fmt.Sprintf("%s-%s", strconv.FormatInt(f.size, 10), hashstr)
			if g, ok := result[key]; ok {
				result[key] = append(g, f)
			} else {
				result[key] = []FileDetail{f}
			}
		}
	}
	for k, v := range result {
		if len(v) <= 1 {
			delete(result, k)
		}
	}
	return result, nil
}

// recursive read all files under given dir
func recursiveReadDir(path string, fds *[]FileDetail) error {
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, file := range files {
		fullpath := filepath.Join(path, file.Name())
		if file.IsDir() {
			recursiveReadDir(fullpath, fds)
		} else {
			fi, _ := file.Info()
			size := fi.Size()
			// 0 size file is lock file, we don't want to consider it for duplication check
			if size > 0 {
				*fds = append(*fds, FileDetail{size: size, path: fullpath})
			}
		}
	}
	return nil
}

// create hash(SHA256) string of file
func hash(fd *FileDetail, quick bool) (string, error) {
	if fd.hash != empty {
		return fd.hash, nil
	}
	fi, err := os.Stat(fd.path)
	if err != nil {
		return empty, err
	}
	size := fi.Size()
	var hashstr string
	if quick && size > samplethreshold && size > samplesize {
		if hashstr, err = hashWithSampling(fd, size); err != nil {
			return empty, err
		}
	} else {
		var b []byte
		if b, err = os.ReadFile(fd.path); err != nil {
			return empty, err
		}
		hashstr = fmt.Sprintf("%x", sha256.Sum256(b))
	}
	fd.hash = hashstr
	return hashstr, nil
}

// hash large file by sampling for better performance
func hashWithSampling(fd *FileDetail, size int64) (string, error) {
	f, err := os.Open(fd.path)
	if err != nil {
		return empty, err
	}
	// sample at beginning of the file
	bb := make([]byte, samplesize)
	s := io.NewSectionReader(f, 0, samplesize)
	s.Read(bb)
	// sample at end of the file
	be := make([]byte, samplesize)
	s.ReadAt(be, size-samplesize)
	// join both samples
	b := append(bb, be...)
	return fmt.Sprintf("%x", sha256.Sum256(b)), nil
}
