package core

import (
	"errors"
	"io"
	"log"
	"sync"
)

type MiniKv struct {
	indexes   map[string]int64
	diskFile  *DiskFile
	compactor *Compactor
	config    *Config
	flusher   *Flusher
	mu        sync.RWMutex
}

func Open(config *Config) (*MiniKv, error) {
	if config == nil {
		log.Fatal("config can not be none")
	}

	diskFile, err := NewDiskFile(config.DataDir)
	if err != nil {
		return nil, err
	}

	compactor := NewCompactor(diskFile)

	mkv := &MiniKv{
		diskFile:  diskFile,
		indexes:   make(map[string]int64),
		compactor: compactor,
		config:    config,
	}

	mkv.loadIndexesFromFile()

	go compactor.Compact()

	return mkv, nil
}

func (mkv MiniKv) Close() {
	mkv.diskFile.Close()
	mkv.compactor.StopRunning()
}

func (mkv *MiniKv) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("key can not be empty")
	}

	mkv.mu.Lock()
	defer mkv.mu.Unlock()

	offset := mkv.diskFile.offset

	entry := NewEntry(key, value, PUT)

	err := mkv.diskFile.Write(entry)
	if err != nil {
		return nil
	}

	mkv.indexes[string(key)] = offset

	return nil
}

func (mkv *MiniKv) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("key can not be empty")
	}

	mkv.mu.Lock()
	defer mkv.mu.Unlock()

	_, ok := mkv.indexes[string(key)]
	if !ok {
		return errors.New("key not exist")
	}

	entry := NewEntry(key, nil, DEL)

	err := mkv.diskFile.Write(entry)
	if err != nil {
		return nil
	}

	delete(mkv.indexes, string(key))

	return nil
}

func (mkv MiniKv) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("key can not be empty")
	}

	mkv.mu.RLock()
	defer mkv.mu.Unlock()

	offset, ok := mkv.indexes[string(key)]
	if !ok {
		return nil, errors.New("key not exist")
	}

	var e *Entry
	e, err := mkv.diskFile.Read(offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return e.Value, nil
}

func (mkv MiniKv) Scan0() {
	// TODO
}

func (mkv MiniKv) Scan(start, stop []byte) {
	// TODO
}

func (mkv *MiniKv) loadIndexesFromFile() {
	if mkv.diskFile == nil {
		return
	}

	var offset int64
	for {
		e, err := mkv.diskFile.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return
		}

		// 设置索引状态
		mkv.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			// 删除内存中的 key
			delete(mkv.indexes, string(e.Key))
		}

		offset += e.GetSize()
	}
	return
}
