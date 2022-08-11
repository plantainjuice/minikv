package core

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mmmmmmmingor/minikv/util"
)

const (
	FILE_NAME_TMP_SUFFIX     = ".tmp"
	FILE_NAME_ARCHIVE_SUFFIX = ".archive"
)

type DiskStore struct {
	maxFileId    int64
	dataDir      string
	diskFiles    []*DiskFile
	maxDiskFiles int
	updateLock   *sync.Mutex
}

func NewDiskStore(dataDir string, maxDiskFiles int) *DiskStore {
	return &DiskStore{
		dataDir:      dataDir,
		diskFiles:    make([]*DiskFile, 0),
		maxDiskFiles: maxDiskFiles,
		updateLock:   &sync.Mutex{},
	}
}

func (ds DiskStore) listDiskFiles() []os.FileInfo {
	files, err := ioutil.ReadDir(ds.dataDir)
	if err != nil {
		log.Fatal(err)
	}

	filesList := make([]os.FileInfo, 0)
	regex, err := regexp.Compile(`data\.([0-9]+)`)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		matched := regex.Match([]byte(f.Name()))
		if matched {
			filesList = append(filesList, f)
		}
	}

	return filesList
}

func (ds DiskStore) GetMaxDiskId() int64 {
	var maxFileId int64 = -1

	files := ds.listDiskFiles()
	for _, file := range files {
		id, _ := strconv.ParseInt(strings.Split(file.Name(), ".")[1], 10, 64)
		if id > maxFileId {
			maxFileId = id
		}
	}

	return maxFileId
}

func (ds DiskStore) NextDiskFileId() int64 {
	ds.updateLock.Lock()
	defer ds.updateLock.Unlock()
	return atomic.AddInt64(&ds.maxFileId, 1)
}

func (ds *DiskStore) AddDiskFile(df *DiskFile) {
	ds.updateLock.Lock()
	defer ds.updateLock.Unlock()
	ds.diskFiles = append(ds.diskFiles, df)
}

func (ds *DiskStore) AddDiskFile1(fileName string) {
	ds.updateLock.Lock()
	defer ds.updateLock.Unlock()
	df := NewDiskFile(fileName)
	ds.diskFiles = append(ds.diskFiles, df)
}

func (ds DiskStore) GetNextDiskFileName() string {
	fname := fmt.Sprintf(ds.dataDir+"/data.%020d", ds.NextDiskFileId())
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Println(f.Name())
	return fname
}

func (ds *DiskStore) Open() {
	if !util.Exists(ds.dataDir) {
		err := os.Mkdir(ds.dataDir, os.ModePerm)
		if err != nil {
			panic("create data dir error")
		}
	}

	files := ds.listDiskFiles()
	for _, f := range files {
		// 没有懒加加载， 一初始化就全部加载到内存了，内存会爆掉
		df := NewDiskFile(f.Name())
		ds.diskFiles = append(ds.diskFiles, df)
	}
	ds.maxFileId = ds.GetMaxDiskId()
}

func (ds DiskStore) Close() {
	for _, df := range ds.diskFiles {
		err := df.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (ds DiskStore) CreateIterator() <-chan *KeyValue {
	return ds.CreateIterator1(ds.diskFiles)
}

func (ds DiskStore) CreateIterator1(diskFiles []*DiskFile) <-chan *KeyValue {
	c := make(chan *KeyValue)
	for _, df := range diskFiles {
		for kv := range df.CreateItertator() {
			c <- kv
		}
	}
	close(c)
	return c
}
