package core

import (
	"log"
	"os"
	"sync"
)

type Flusher struct {
	diskStore *DiskStore
	wg        sync.WaitGroup
}

func NewFlusher(diskStore *DiskStore) *Flusher {
	return &Flusher{
		diskStore: diskStore,
	}
}

func (f Flusher) Flush(it *SkipList) error {
	f.wg.Add(1)

	defer f.wg.Done()

	fileName := f.diskStore.GetNextDiskFileName()
	fileTmpName := fileName + FILE_NAME_TMP_SUFFIX

	writer := NewDiskFileWriter(fileTmpName)
	defer os.Remove(fileTmpName)

	for i := range it.Iterator() {
		log.Println("Flush", i.key, i.value)
		writer.Append(i)
	}
	writer.AppendIndex()
	writer.AppendTrailer()
	writer.Close()

	err := os.Rename(fileTmpName, fileName)
	if err != nil {
		log.Println(err)
		return err
	}

	f.diskStore.AddDiskFile1(fileName)

	return nil
}

func (f Flusher) Wait() {
	f.wg.Wait()
}
