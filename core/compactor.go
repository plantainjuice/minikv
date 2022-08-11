package core

import (
	"log"
	"os"
	"time"
)

type Compactor struct {
	diskStore *DiskStore
	running   bool
}

func NewCompactor(ds *DiskStore) *Compactor {
	return &Compactor{
		diskStore: ds,
		running:   true,
	}
}

func (c *Compactor) performCompact(filesToCompact []*DiskFile) error {
	fileName := c.diskStore.GetNextDiskFileName()
	fileTmpName := fileName + FILE_NAME_TMP_SUFFIX

	writer := NewDiskFileWriter(fileTmpName)
	iter := c.diskStore.CreateIterator1(filesToCompact)
	// TODO compact logic
	for kv := range iter {
		log.Println("Compact", kv.key, kv.value)
		writer.Append(kv)
	}

	writer.AppendIndex()
	writer.AppendTrailer()
	writer.Close()

	err := os.Rename(fileTmpName, fileName)
	if err != nil {
		log.Println(err)
		return err
	}

	c.diskStore.AddDiskFile1(fileName)

	return nil
}

func (c *Compactor) Compact() {
	for c.running {
		isCompacted := false

		filesToCompact := make([]*DiskFile, 0)
		filesToCompact = append(filesToCompact, c.diskStore.diskFiles...)
		if len(c.diskStore.diskFiles) > c.diskStore.maxDiskFiles {
			c.performCompact(filesToCompact)
			isCompacted = false
		}

		if !isCompacted {
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *Compactor) StopRunning() {
	c.running = false
}
