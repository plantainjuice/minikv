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

	if os.Rename(fileTmpName, fileName) != nil {
		log.Fatal("Rename " + fileTmpName + " to " +
			fileName + " failed when flushing")
		return errors.New("flush error")
	}

	f.diskStore.AddDiskFile1(fileName)

	os.Remove(fileTmpName)
	return nil
}
