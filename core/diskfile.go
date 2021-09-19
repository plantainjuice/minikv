package core

import (
	"log"
	"os"

	"github.com/mmmmmmmingor/minikv/util"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	BLOCK_SIZE_UP_LIMIT              = 1024 * 1024 * 2
	BLOOM_FILTER_HASH_COUNT          = 3
	BLOOM_FILTER_BITS_PER_KEY        = 10
	TRAILER_SIZE                     = 8 + 4 + 8 + 8 + 8
	DISK_FILE_MAGIC           uint64 = 18070835493257478057 // 0xFAC881234221FFA9L
)

type Void struct{}

type DiskFile struct {
	fname            string
	in               *os.File
	fileSize         uint64
	blockCount       uint32
	blockIndexOffset uint64
	blockIndexSize   uint64
	blockMetaSet     map[*BlockMeta]Void
}

func NewDiskFile(filename string) *DiskFile {
	df := &DiskFile{
		blockMetaSet: make(map[*BlockMeta]Void), // map 也要初始化
	}
	df.Open(filename)
	return df
}

func (df *DiskFile) CreateItertator() <-chan *KeyValue {
	c := make(chan *KeyValue)
	go func() {
		// 分别把每一个 blockMeta 的每一个 kv 迭代一遍
		for blockMeta := range df.blockMetaSet {
			blockReader := df.load(blockMeta)
			for _, kv := range blockReader.KvBuf {
				c <- kv
			}
		}
		close(c)
	}()
	return c
}

func (df *DiskFile) load(meta *BlockMeta) (blockReader *BlockReader) {

	buffer := make([]byte, meta.BlockSize)
	length, err := df.in.ReadAt(buffer, int64(meta.BlockOffset))
	if err != nil || length != len(buffer) {
		logrus.Error("read from file error")
		panic(err)
	}
	return BlockReaderParseFrom(buffer, 0, length)
}

func (df *DiskFile) Open(filename string) {
	df.fname = filename
	logrus.Info("open file: ", filename)

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalln("open file fail")
	}
	df.in = file

	stat, err := file.Stat()
	df.fileSize = uint64(stat.Size())

	if err != nil {
		log.Fatalln("oepn stat err")
	} else if df.fileSize < TRAILER_SIZE {
		log.Fatalln("filesize < TRAILER_SIZE")
	}

	offset := int64(df.fileSize - TRAILER_SIZE)

	buffer := make([]byte, 8)
	_, err = df.in.ReadAt(buffer, offset)
	if err != nil || df.fileSize != util.BytesToUint64(buffer) {
		log.Fatalln("read filesize error")
	}

	offset += 8

	buffer = make([]byte, 4)
	_, err = df.in.ReadAt(buffer, offset)
	if err != nil {
		log.Fatalln("read blockCount error")
	}
	offset += 4
	df.blockCount = util.BytesToUint32(buffer)

	buffer = make([]byte, 8)
	_, err = df.in.ReadAt(buffer, offset)
	if err != nil {
		log.Fatalln("read blockIndexOffset error")
	}
	offset += 8
	df.blockIndexOffset = util.BytesToUint64(buffer)

	buffer = make([]byte, 8)
	_, err = df.in.ReadAt(buffer, offset)
	if err != nil {
		log.Fatalln("read blockIndexSize error")
	}
	offset += 8
	df.blockIndexSize = util.BytesToUint64(buffer)

	buffer = make([]byte, 8)
	_, err = df.in.ReadAt(buffer, offset)
	if err != nil || DISK_FILE_MAGIC != util.BytesToUint64(buffer) {
		log.Fatalln("read Magic number error")
	}
	offset += 8

	buffer = make([]byte, df.blockIndexSize)
	offset = int64(df.blockIndexOffset)
	df.in.ReadAt(buffer, offset)

	haveRead := 0
	for true {
		meta := ParseFrom(buffer)
		var void Void
		df.blockMetaSet[meta] = void
		haveRead += meta.GetSerializeSize()
		// 解析出错也许会死循环
		if haveRead >= len(buffer) {
			break
		}
		//要这个之后的,每次都把开头的解析掉, 这里不用加一开始字段
		buffer = buffer[meta.GetSerializeSize():]
	}
}

func (df *DiskFile) Close() error {
	err := df.in.Close()
	return err
}
