package core

import (
	"hash/crc32"
	"log"

	"github.com/mmmmmmmingor/minikv/util"
)

type BlockReader struct {
	KvBuf []*KeyValue
}

func NewBlockReader(kvBuf []*KeyValue) *BlockReader {
	return &BlockReader{
		KvBuf: kvBuf,
	}
}

func BlockReaderParseFrom(buffer []byte, offset, size int) *BlockReader {
	pos := 0
	kvBuf := make([]*KeyValue, 0)
	crc := crc32.IEEETable
	var checkSum1 uint32 = 0

	// Parse kv getSerializeSize
	buf := buffer[pos+offset : pos+KV_SIZE_LEN]
	kvSize := util.BytesToInt32(buf)
	pos += int(kvSize)

	// Parse all key value.
	for i := 0; i < int(kvSize); i++ {
		buf := buffer[pos+offset:]
		kv := ParseFrom1(buf)

		kvBuf = append(kvBuf, &kv)

		buf = buffer[pos+offset : pos+int(kv.GetSerializeSize())]
		checkSum1 = crc32.Checksum(buf, crc)

		pos += int(kv.GetSerializeSize())
	}

	// Parse checksum
	buf = buffer[pos+offset : pos+CHECKSUM_LEN]
	checkSum2 := util.BytesToUint32(buf)
	pos += CHECKSUM_LEN

	if checkSum1 != checkSum2 {
		log.Fatal("checksum not equal", checkSum1, checkSum2)
	}

	if size != pos {
		log.Fatal("size and pos not equal", size, pos)
	}

	return NewBlockReader(kvBuf)
}
