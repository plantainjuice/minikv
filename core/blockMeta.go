package core

import (
	"github.com/mmmmmmmingor/minikv/util"
)

const (
	OFFSET_SIZE = 8
	SIZE_SIZE   = 8
	BF_LEN_SIZE = 4
)

type BlockMeta struct {
	LastKV      KeyValue
	BlockOffset uint64
	BlockSize   uint64
	Bloomfilter []byte
}

func NewBlockMeta(lastKv KeyValue, blockOffset, blockSize uint64, bloomfilter []byte) *BlockMeta {
	return &BlockMeta{
		LastKV:      lastKv,
		BlockOffset: blockOffset,
		BlockSize:   blockOffset,
		Bloomfilter: bloomfilter,
	}
}

func CreateSeekDummy(lastKv KeyValue) *BlockMeta {
	return NewBlockMeta(lastKv, 0, 0, []byte{})
}

func (bm BlockMeta) GetSerializeSize() int {
	return int(bm.LastKV.GetSerializeSize()) + OFFSET_SIZE + SIZE_SIZE + BF_LEN_SIZE + len(bm.Bloomfilter)

}

func (bm BlockMeta) ToBytes() []byte {
	bytes := make([]byte, bm.GetSerializeSize())
	pos := 0

	// key-value
	buffer, _ := bm.LastKV.ToBytes()
	copy(bytes[pos:pos+len(buffer)], buffer)
	pos += len(buffer)

	// block offset
	buffer = util.Uint64ToBytes(bm.BlockOffset)
	copy(bytes[pos:pos+len(buffer)], buffer)
	pos += len(buffer)

	// block size
	buffer = util.Uint64ToBytes(bm.BlockSize)
	copy(bytes[pos:pos+len(buffer)], buffer)
	pos += len(buffer)

	// bloom filter len
	buffer = util.Uint32ToBytes(uint32(len(bm.Bloomfilter)))
	copy(bytes[pos:pos+len(buffer)], buffer)
	pos += len(buffer)

	// bloom filter
	copy(bytes[pos:pos+len(bm.Bloomfilter)], bm.Bloomfilter)

	return bytes
}

func ParseFrom(bytes []byte) *BlockMeta {
	pos := 0

	lastKv := ParseFrom1(bytes)
	pos += int(lastKv.GetSerializeSize())

	blockOffset := util.BytesToUint64(bytes[pos : pos+OFFSET_SIZE])
	pos += OFFSET_SIZE

	blockSize := util.BytesToUint64(bytes[pos : pos+SIZE_SIZE])
	pos += SIZE_SIZE

	bloomFilterSize := util.BytesToUint32(bytes[pos : pos+BF_LEN_SIZE])
	pos += BF_LEN_SIZE

	bloomFilter := bytes[pos : pos+int(bloomFilterSize)]

	return NewBlockMeta(lastKv, blockOffset, blockSize, bloomFilter)
}
