package diskstore

type Flusher struct {
	diskStore *DiskStore
}

func NewFlusher(diskStore *DiskStore) *Flusher {
	return &Flusher{
		diskStore: diskStore,
	}
}

func (flusher Flusher) Flush() {
	// TODO
}
