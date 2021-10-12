package core

import (
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

	writer := NewDiskFileWriter(fileTmpName)

	for i := range it.Iterator() {
		log.Println(i.key, i.value)
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

	os.Remove(fileTmpName)
	return nil
}
