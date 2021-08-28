package test

import (
	"testing"

	"github.com/mmmmmmmingor/minikv/core"
	"github.com/mmmmmmmingor/minikv/util"
	"github.com/stretchr/testify/assert"
)

func TestBlockEncoding(t *testing.T) {
	bw := core.NewBlockWriter()
	var lastBytes []byte

	for i := int32(0); i < 100; i++ {
		lastBytes = util.Int32ToBytes(i)
		kv := core.NewKeyValue(lastBytes, lastBytes, core.PUT, uint64(1))
		bw.Append(&kv)
	}

	kv := core.NewKeyValue(lastBytes, lastBytes, core.PUT, uint64(1))
	assert.Equal(t, bw.LastKV, &kv, "lastKv not equal")

	buffer := bw.Serialize()
	br := core.BlockReaderParseFrom(buffer, 0, len(buffer))

	// Assert the bloom filter.
	bytes := make([][]byte, len(br.KvBuf))

	for i := int(0); i < len(br.KvBuf); i++ {
		bytes[i] = br.KvBuf[i].GetKey()
	}

	bloom := util.NewBloomFilter(core.BLOOM_FILTER_HASH_COUNT, core.BLOOM_FILTER_BITS_PER_KEY)
	assert.Equal(t, bloom.Generate(bytes), bw.BloomFilter, "bloomfilter not equal")
}

func TestBlockMeta(t *testing.T) {
	lastKv := core.NewKeyValue([]byte("key"), []byte("value"), core.PUT, 3)
	offset := uint64(1024)
	size := uint64(1024)
	bloomFilter := []byte("bloomFilter")

	mata := core.NewBlockMeta(lastKv, offset, size, bloomFilter)
	buffer := mata.ToBytes()

	meta2 := core.ParseFrom(buffer)

	assert.Equal(t, lastKv, meta2.LastKV, "lastkv not equal")
	assert.Equal(t, offset, meta2.BlockOffset, "offset not equal")
	assert.Equal(t, size, meta2.BlockSize, "size not equal")
	assert.Equal(t, bloomFilter, meta2.Bloomfilter, "bloomfilter not equal")
}

func TestDiskFile(t *testing.T) {
	dbFile := "testDiskFileWriter.db"

	diskFileWriter := core.NewDiskFileWriter(dbFile)

	for i := 0; i < 1000; i++ {
		kv := core.NewKeyValue([]byte("1"), []byte("2"), core.PUT, 1)
		diskFileWriter.Append(&kv)
	}

	diskFileWriter.AppendIndex()
	diskFileWriter.AppendTrailer()

	diskFile := core.NewDiskFile(dbFile)
	diskFile.Close()
}

func TestDiskFileIO(t *testing.T) {
	dbFile := "testDiskFileIO.db"

	diskFileWriter := core.NewDiskFileWriter(dbFile)

	for i := 0; i < 1000; i++ {
		kv := core.NewKeyValue([]byte("1"), []byte("2"), core.PUT, 1)
		diskFileWriter.Append(&kv)
	}

	diskFileWriter.AppendIndex()
	diskFileWriter.AppendTrailer()

	// diskFile := core.NewDiskFile(dbFile)
	// diskFile.iterator()
}
