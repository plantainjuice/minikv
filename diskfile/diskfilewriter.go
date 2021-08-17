package diskfile

type DiskFileWriter struct {
	fname         string
	currentOffset int64
	indexWriter   *BlockIndexWriter
	currentWriter *BlockWriter

	fileSize         int64
	blockCount       int
	blockIndexOffset int64
	blockIndexSize   int64
}

func NewDiskFileWriter(fname string) *DiskFileWriter {
	// file := os.Op

	return &DiskFileWriter{
		fname:            fname,
		fileSize:         0,
		blockCount:       0,
		blockIndexOffset: 0,
		blockIndexSize:   0,
		currentOffset:    0,
		indexWriter:      NewBlockIndexWriter(),
	}
}
