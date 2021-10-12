package core

import (
	"errors"
	"log"
	"os"
)

type Flusher struct {
	diskStore *DiskStore
}

func NewFlusher(diskStore *DiskStore) *Flusher {
	return &Flusher{
		diskStore: diskStore,
	}
}

func (f Flusher) Flush(it *SkipList) error {
	fileName := f.diskStore.GetNextDiskFileName()
	fileTmpName := fileName + FILE_NAME_TMP_SUFFIX

	writer := NewDiskFileWriter(fileName)

	for i := range it.Iterator() {
		writer.Append(i)
	}
	writer.AppendIndex()
	writer.AppendTrailer()

	err := os.Rename(fileTmpName, fileName)

	if err != nil {
		log.Fatal(err)
		return errors.New("flush error")
	}

	f.diskStore.AddDiskFile1(fileName)

	os.Remove(fileTmpName)
	return nil
}
