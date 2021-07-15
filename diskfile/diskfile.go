package diskfile

import (
	"errors"	"os"
)


const (
	BLOCK_SIZE_UP_LIMIT       = 1024 * 1024 * 2
	BLOOM_FILTER_HASH_COUNT   = 3
	BLOOM_FILTER_BITS_PER_KEY = 10
	TRAILER_SIZE              = 8 + 4 + 8 + 8 + 8
	DISK_FILE_MAGIC           = 18070835493257478057 // 0xFAC881234221FFA9L
)

type DiskFile struct {
	fname            string
	in               os.File
	fileSize         uint64
	blockCount       uint32
	blockIndexOffset uint64
	blockIndexSize   uint64
}

func (df *DiskFile) Open(filename string) error {
	df.fname = filename

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	df.in = file

	stat, err := file.Stat()
	df.fileSize = stat.Size()

	if df.fileSize < TRAILER_SIZE{
		return errors.New("filesize < TRAILER_SIZE")
	}

	offset := fd.fileSize - TRAILER_SIZE

}
