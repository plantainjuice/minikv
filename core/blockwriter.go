package core

import (
	"hash/crc32"

	"github.com/mmmmmmmingor/minikv/util"
)

const (
	KV_SIZE_LEN  = 4
	CHECKSUM_LEN = 4
)

type BlockWriter struct {
	TotalSize     int
	KvBuf         []*KeyValue
	BloomFilter   util.BloomFilter
	Crc           *crc32.Table
	CheckSum      uint32
	LastKV        *KeyValue
	KeyValueCount int
}

func NewBlockWriter() *BlockWriter {
	return &BlockWriter{
		TotalSize:     0,
		KeyValueCount: 0,
		KvBuf:         make([]*KeyValue, 0),
		BloomFilter: *util.NewBloomFilter(
			BLOOM_FILTER_HASH_COUNT,
			BLOOM_FILTER_BITS_PER_KEY),
		Crc: crc32.IEEETable,
	}
}

func (bw *BlockWriter) GenerateBloomFilter() []byte {
	keys := make([][]byte, len(bw.KvBuf))
	for i := range bw.KvBuf {
		keys = append(keys, bw.KvBuf[i].GetKey())
	}
	return bw.BloomFilter.Generate(keys)
}

func (bw *BlockWriter) Append(kv *KeyValue) {
	bw.KvBuf = append(bw.KvBuf, kv)
	bw.LastKV = kv

	buf, _ := kv.ToBytes()
	bw.CheckSum = crc32.Checksum(buf, bw.Crc)

	bw.TotalSize += int(kv.GetSerializeSize())
	bw.KeyValueCount += 1
}

func (bw BlockWriter) Size() int {
	return KV_SIZE_LEN + bw.TotalSize + CHECKSUM_LEN
}

func (bw *BlockWriter) Serialize() []byte {
	buffer := make([]byte, bw.Size())
	pos := 0

	kvSize := util.Int32ToBytes(int32(len(bw.KvBuf)))
	copy(buffer[pos:pos+KV_SIZE_LEN], kvSize)
	pos += KV_SIZE_LEN

	for _, kv := range bw.KvBuf {
		buf, _ := kv.ToBytes()
		copy(buffer[pos:pos+len(buf)], buf)
		pos += len(buf)
	}

	copy(buffer[pos:pos+CHECKSUM_LEN], util.Uint32ToBytes(bw.CheckSum))

	return buffer
}
