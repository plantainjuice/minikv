package diskfile

import (
	"log"
	"os"

	"github.com/mmmmmmmingor/minikv"
)

const (
	BLOCK_SIZE_UP_LIMIT              = 1024 * 1024 * 2
	BLOOM_FILTER_HASH_COUNT          = 3
	BLOOM_FILTER_BITS_PER_KEY        = 10
	TRAILER_SIZE                     = 8 + 4 + 8 + 8 + 8
	DISK_FILE_MAGIC           uint64 = 18070835493257478057 // 0xFAC881234221FFA9L
)

type DiskFile struct {
	fname            string
	in               *os.File
	fileSize         uint64
	blockCount       uint32
	blockIndexOffset uint64
	blockIndexSize   uint64
}

func (df *DiskFile) Open(filename string) {
	df.fname = filename

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalln("open file fail")
	}
	df.in = file

	stat, err := file.Stat()
	df.fileSize = uint64(stat.Size())

	if df.fileSize < TRAILER_SIZE {
		log.Fatalln("filesize < TRAILER_SIZE")
	}

	offset := int64(df.fileSize - TRAILER_SIZE)

	buffer := make([]byte, 8)
	_, err = df.in.ReadAt(buffer, offset)
	if err != nil || df.fileSize != minikv.BytesToUint64(buffer) {
		log.Fatalln("read filesize error")
	}
	offset += 8

	buffer = make([]byte, 4)
	_, err = df.in.ReadAt(buffer, offset)
	if err != nil {
		log.Fatalln("read blockCount error")
	}
	offset += 4
	df.blockCount = minikv.BytesToUint32(buffer)

	buffer = make([]byte, 8)
	_, err = df.in.ReadAt(buffer, offset)
	if err != nil {
		log.Fatalln("read blockIndexOffset error")
	}
	offset += 8
	df.blockIndexOffset = minikv.BytesToUint64(buffer)

	buffer = make([]byte, 8)
	_, err = df.in.ReadAt(buffer, offset)
	if err != nil {
		log.Fatalln("read blockIndexSize error")
	}
	offset += 8
	df.blockIndexSize = minikv.BytesToUint64(buffer)

	buffer = make([]byte, 8)
	_, err = df.in.ReadAt(buffer, offset)
	if err != nil || DISK_FILE_MAGIC != minikv.BytesToUint64(buffer) {
		log.Fatalln("read Magic number error")
	}
	offset += 8

	buffer = make([]byte, df.blockIndexSize)
	offset = int64(df.blockIndexOffset)
	df.in.ReadAt(buffer, int64(df.blockIndexOffset))

	haveRead := 0
	for haveRead < len(buffer) {
		var meta BlockMeta = ParseFrom(buffer)
		haveRead += meta.GetSerializeSize()
	}
}
