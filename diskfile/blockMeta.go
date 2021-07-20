package diskfile

import (
	"github.com/mmmmmmmingor/minikv"
)

const (
	OFFSET_SIZE = 8
	SIZE_SIZE   = 8
	BF_LEN_SIZE = 4
)

type BlockMeta struct {
	lastKV      minikv.KeyValue
	blockOffset uint64
	blockSize   uint64
	bloomfilter []byte
}

func New(lastKv minikv.KeyValue, blockOffset, blockSize uint64, bloomfilter []byte) BlockMeta {
	return BlockMeta{
		lastKV:      lastKv,
		blockOffset: blockOffset,
		blockSize:   blockOffset,
		bloomfilter: bloomfilter,
	}
}

func CreateSeekDummy(lastKv minikv.KeyValue) BlockMeta {
	return New(lastKv, 0, 0, []byte{})
}

func (bm BlockMeta) GetSerializeSize() int {
	return int(bm.lastKV.GetSerializeSize()) + OFFSET_SIZE + SIZE_SIZE + BF_LEN_SIZE + len(bm.bloomfilter)

}

func (bm BlockMeta) ToBytes() []byte {
	bytes := make([]byte, bm.GetSerializeSize())
	pos := 0

	// key-value
	buffer, _ := bm.lastKV.ToBytes()
	copy(bytes[pos:pos+len(buffer)], buffer)
	pos += len(buffer)

	// block offset
	buffer = minikv.Uint64ToBytes(bm.blockOffset)
	copy(bytes[pos:pos+len(buffer)], buffer)
	pos += len(buffer)

	// block size
	buffer = minikv.Uint64ToBytes(bm.blockSize)
	copy(bytes[pos:pos+len(buffer)], buffer)
	pos += len(buffer)

	// bloom filter len
	buffer = minikv.Uint32ToBytes(uint32(len(bm.bloomfilter)))
	copy(bytes[pos:pos+len(buffer)], buffer)
	pos += len(buffer)

	// bloom filter
	copy(bytes[pos:pos+len(bm.bloomfilter)], bm.bloomfilter)

	return bytes
}

func ParseFrom(bytes []byte) BlockMeta {
	pos := 0

	lastKv := minikv.ParseFrom(bytes)
	pos += int(lastKv.GetSerializeSize())

	blockOffset := minikv.BytesToUint64(bytes[pos : pos+OFFSET_SIZE])
	pos += OFFSET_SIZE

	blockSize := minikv.BytesToUint64(bytes[pos : pos+SIZE_SIZE])
	pos += SIZE_SIZE

	bloomFilterSize := minikv.BytesToUint32(bytes[pos : pos+BF_LEN_SIZE])
	pos += BF_LEN_SIZE

	bloomFilter := bytes[pos : pos+int(bloomFilterSize)]

	return New(lastKv, blockOffset, blockSize, bloomFilter)
}
