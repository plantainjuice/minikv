package core

import (
	"time"
)

type Compactor struct {
	diskfile *DiskFile
	running  bool
}

func NewCompactor(ds *DiskFile) *Compactor {
	return &Compactor{
		diskfile: ds,
		running:  true,
	}
}

func (c *Compactor) performCompact() error {
	return nil
}

func (c *Compactor) Compact() {
	for c.running {
		isCompacted := false

		c.performCompact()

		if !isCompacted {
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *Compactor) StopRunning() {
	c.running = false
}
