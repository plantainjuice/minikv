package core

import (
	"os"
)

const (
	FILE_NAME = "minikv.data"
)

type DiskFile struct {
	file   *os.File
	offset int64
}

func NewDiskFile(path string) (*DiskFile, error) {
	fileName := path + string(os.PathSeparator) + FILE_NAME
	return newDiskFile(fileName)
}

func newDiskFile(filename string) (*DiskFile, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	return &DiskFile{
		offset: stat.Size(),
		file:   file,
	}, nil
}

func (df *DiskFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	if _, err = df.file.ReadAt(buf, offset); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}

	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.file.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.file.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}

func (df *DiskFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.file.WriteAt(enc, df.offset)
	df.offset += e.GetSize()
	return
}

func (df *DiskFile) Close() error {
	err := df.file.Close()
	return err
}
