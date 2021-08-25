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

func (c *Compactor) performCompact(filesToCompact []*DiskFile) {
	fileName := c.diskStore.GetNextDiskFileName()
	fileTmpName := fileName + FILE_NAME_TMP_SUFFIX

	{
		writer := NewDiskFileWriter(fileTmpName)
		// TODO
		c.diskStore.CreateIterator()

		writer.Close()
	}
}

func (c *Compactor) Compact() {
	filesToCompact := make([]*DiskFile, 0)
	filesToCompact = append(filesToCompact, c.diskStore.diskFiles...)
	c.performCompact(filesToCompact)
}
