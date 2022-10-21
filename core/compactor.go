package core

import (
	"time"
)

type Compactor struct {
	mkv     *MiniKv
	running bool
}

func NewCompactor() *Compactor {
	return &Compactor{
		running: false,
	}
}

func (c *Compactor) performCompact() {
	c.mkv.Merge()
}

func (c *Compactor) Compact(mkv *MiniKv) {
	c.mkv = mkv

	for c.running {
		time.Sleep(1 * time.Second)

		c.performCompact()
	}
}

func (c *Compactor) StopRunning() {
	c.running = false
}
