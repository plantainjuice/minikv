package diskstore

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

	"github.com/mmmmmmmingor/minikv/diskfile"
)

const (
	FILE_NAME_TMP_SUFFIX     = ".tmp"
	FILE_NAME_ARCHIVE_SUFFIX = ".archive"
)

type DiskStore struct {
	dataDir      string
	diskFiles    []*diskfile.DiskFile
	maxDiskFiles int
	maxFileId    int64
	updateLock   *sync.Mutex
}

func NewDiskStore(dataDir string, maxDiskFiles int) DiskStore {
	return DiskStore{
		dataDir:      dataDir,
		diskFiles:    make([]*diskfile.DiskFile, 0),
		maxDiskFiles: maxDiskFiles,
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

func (ds *DiskStore) AddDiskFile(df *diskfile.DiskFile) {
	ds.updateLock.Lock()
	defer ds.updateLock.Unlock()
	ds.diskFiles = append(ds.diskFiles, df)
}

func (ds *DiskStore) AddDiskFile1(fileName string) {
	df := diskfile.NewDiskFile(fileName)
	ds.diskFiles = append(ds.diskFiles, df)
}

func (ds DiskStore) GetNextDiskFileName() string {
	f, err := os.Create(fmt.Sprintf("data.%020d", ds.NextDiskFileId()))
	if err != nil {
		log.Fatal(err)
	}
	return f.Name()
}

func (ds *DiskStore) Open() {
	files := ds.listDiskFiles()
	for _, f := range files {
		df := diskfile.NewDiskFile(f.Name())
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
