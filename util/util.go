package util

import (
	"encoding/binary"
	"os"
	"path/filepath"
)

func Hash(key []byte) int {
	if len(key) == 0 {
		return 0
	}
	h := 1
	for i := 0; i < len(key); i++ {
		h = (h << 5) + h + int(key[i])
	}
	return h
}

func Uint8ToBytes(i uint8) []byte {
	bytes := make([]byte, 1)
	bytes[0] = i
	return bytes
}

func Int32ToBytes(i int32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(i))
	return bytes
}

func Uint32ToBytes(i uint32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, i)
	return bytes
}

func Uint64ToBytes(i uint64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, i)
	return bytes
}

func BytesToUint32(buf []byte) uint32 {
	return binary.BigEndian.Uint32(buf)
}

func BytesToUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

func BytesToInt32(buf []byte) int32 {
	return int32(binary.BigEndian.Uint32(buf))
}

func BytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

func Exists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

// 判断所给路径是否为文件夹
func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

// 判断所给路径是否为文件
func IsFile(path string) bool {
	return !IsDir(path)
}

func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}
