package core

import (
	"log"
	"os"

	"github.com/mmmmmmmingor/minikv/util"
)

type DiskFileWriter struct {
	fname            string
	currentOffset    uint64
	indexWriter      *BlockIndexWriter
	currentWriter    *BlockWriter
	out              *os.File
	fileSize         uint64
	blockCount       uint32
	blockIndexOffset uint64
	blockIndexSize   uint64
}

func NewDiskFileWriter(fname string) *DiskFileWriter {
	outFile, err := os.OpenFile(fname, os.O_RDWR, 0644)

	if err != nil {
		log.Fatal(err)
	}

	return &DiskFileWriter{
		fname:            fname,
		fileSize:         0,
		blockCount:       0,
		blockIndexOffset: 0,
		blockIndexSize:   0,
		currentOffset:    0,
		indexWriter:      NewBlockIndexWriter(),
		currentWriter:    NewBlockWriter(),
		out:              outFile,
	}
}

func (dfw *DiskFileWriter) switchNextBlockWriter() {
	if dfw.currentWriter.LastKV == nil {
		log.Fatal("laskKV can not be nil")
	}

	bloomFilter := dfw.currentWriter.GenerateBloomFilter()

	buffer := dfw.currentWriter.Serialize()
	dfw.indexWriter.append(*dfw.currentWriter.LastKV, dfw.currentOffset,
		uint64(len(buffer)), bloomFilter)

	dfw.currentOffset += uint64(len(buffer))
	dfw.blockCount += 1

	dfw.currentWriter = NewBlockWriter()
}

func (dfw *DiskFileWriter) Append(kv *KeyValue) {
	if kv.GetSerializeSize()+KV_SIZE_LEN+CHECKSUM_LEN > BLOCK_SIZE_UP_LIMIT {
		log.Fatal("DiskFileWriter Append")
	}

	if (dfw.currentWriter.KeyValueCount > 0) &&
		(kv.GetSerializeSize()+uint32(dfw.currentWriter.Size()) > BLOCK_SIZE_UP_LIMIT) {
		dfw.switchNextBlockWriter()
	}

	dfw.currentWriter.Append(kv)
}

func (dfw *DiskFileWriter) AppendIndex() {
	if dfw.currentWriter.KeyValueCount > 0 {
		dfw.switchNextBlockWriter()
	}

	buffer := dfw.indexWriter.serialize()
	dfw.blockIndexOffset = dfw.currentOffset
	dfw.blockIndexSize = uint64(len(buffer))

	n, err := dfw.out.Write(buffer)
	if err != nil {
		log.Fatal(err)
	}

	dfw.currentOffset += uint64(n)
}

func (dfw *DiskFileWriter) AppendTrailer() {
	dfw.fileSize = dfw.currentOffset + TRAILER_SIZE

	// fileSize(8B)
	buffer := util.Uint64ToBytes(dfw.fileSize)
	dfw.out.Write(buffer)

	// blockCount(4B)
	buffer = util.Uint32ToBytes(dfw.blockCount)
	dfw.out.Write(buffer)

	// blockIndexOffset(8B)
	buffer = util.Uint64ToBytes(dfw.blockIndexOffset)
	dfw.out.Write(buffer)

	// blockIndexSize(8B)
	buffer = util.Uint64ToBytes(dfw.blockIndexSize)
	dfw.out.Write(buffer)

	// DISK_FILE_MAGIC(8B)
	buffer = util.Uint64ToBytes(DISK_FILE_MAGIC)
	dfw.out.Write(buffer)
}

func (dfw *DiskFileWriter) Close() {
	if dfw.out != nil {
		dfw.out.Sync()
		dfw.out.Close()
	}
}
