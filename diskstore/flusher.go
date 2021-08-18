package diskstore

import (
	"log"
	"os"

	"github.com/mmmmmmmingor/minikv/core/entry"
	"github.com/mmmmmmmingor/minikv/diskfile"
)

type Flusher struct {
	diskStore *DiskStore
}

func NewFlusher(diskStore *DiskStore) *Flusher {
	return &Flusher{
		diskStore: diskStore,
	}
}

func (f Flusher) Flush(it *entry.Iterator) {
	fileName := f.diskStore.GetNextDiskFileName()
	fileTmpName := fileName + FILE_NAME_TMP_SUFFIX

	writer := diskfile.NewDiskFileWriter(fileName)
	for it.HasNext() {
		writer.Append(it.Next())
	}
	writer.AppendIndex()
	writer.AppendTrailer()

	if os.Rename(fileTmpName, fileName) != nil {
		log.Fatal("Rename " + fileTmpName + " to " +
			fileName + " failed when flushing")
	}

	f.diskStore.AddDiskFile1(fileName)

	os.Remove(fileTmpName)
}
