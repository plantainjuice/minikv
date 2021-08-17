package diskfile

import (
	"hash/crc32"

	"github.com/mmmmmmmingor/minikv/core"
	"github.com/mmmmmmmingor/minikv/util"
)

const (
	KV_SIZE_LEN  = 4
	CHECKSUM_LEN = 4
)

type BlockWriter struct {
	totalSize     int
	kvBuf         []*core.KeyValue
	bloomFilter   util.BloomFilter
	crc           *crc32.Table
	checkSum      uint32
	lastKV        *core.KeyValue
	keyValueCount int
}

func NewBlockWriter() *BlockWriter {
	return &BlockWriter{
		totalSize:     0,
		keyValueCount: 0,
		kvBuf:         make([]*core.KeyValue, 0),
		bloomFilter: *util.NewBloomFilter(
			BLOOM_FILTER_HASH_COUNT,
			BLOOM_FILTER_BITS_PER_KEY),
		crc: crc32.IEEETable,
	}
}

func (bw *BlockWriter) Append(kv *core.KeyValue) {
	bw.kvBuf = append(bw.kvBuf, kv)
	bw.lastKV = kv

	buf, _ := kv.ToBytes()
	bw.checkSum = crc32.Checksum(buf, bw.crc)

	bw.totalSize += int(kv.GetSerializeSize())
	bw.keyValueCount += 1
}

func (bw BlockWriter) Size() int {
	return KV_SIZE_LEN + bw.totalSize + CHECKSUM_LEN
}

func (bw *BlockWriter) Serialize() []byte {
	buffer := make([]byte, bw.Size())
	pos := 0

	kvSize := util.Int32ToBytes(int32(len(bw.kvBuf)))
	copy(buffer[pos:pos+KV_SIZE_LEN], kvSize)
	pos += KV_SIZE_LEN

	for _, kv := range bw.kvBuf {
		buf, _ := kv.ToBytes()
		copy(buffer[pos:pos+len(buf)], buf)
		pos += len(buf)
	}

	copy(buffer[pos:pos+CHECKSUM_LEN], util.Uint32ToBytes(bw.checkSum))

	return buffer
}
