package core

import (
	"sync"
)

type Flusher struct {
	diskFile *DiskFile
	wg       sync.WaitGroup
}

func NewFlusher(diskFile *DiskFile) *Flusher {
	return &Flusher{
		diskFile: diskFile,
	}
}

func (f Flusher) Flush() error {
	f.wg.Add(1)

	defer f.wg.Done()

	return nil
}

func (f Flusher) Wait() {
	f.wg.Wait()
}
