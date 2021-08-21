package core

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

func (c *Compactor) PerformCompact() {
	// TODO
}

func (c *Compactor) Compact() {
	// TODO
}
