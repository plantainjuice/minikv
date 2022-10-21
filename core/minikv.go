package core

import (
	"errors"
	"io"
	"log"
	"os"
	"sync"
)

type MiniKv struct {
	indexes   map[string]int64
	diskFile  *DiskFile
	compactor *Compactor
	config    *Config
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

	compactor := NewCompactor()

	mkv := &MiniKv{
		diskFile:  diskFile,
		indexes:   make(map[string]int64),
		compactor: compactor,
		config:    config,
	}

	mkv.loadIndexesFromFile()

	go compactor.Compact(mkv)

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

// Merge 合并数据文件，在rosedb当中是 Reclaim 方法
func (mkv *MiniKv) Merge() error {
	// 没有数据，忽略
	if mkv.diskFile.offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	// 读取原数据文件中的 Entry
	for {
		e, err := mkv.diskFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 内存中的索引状态是最新的，直接对比过滤出有效的 Entry
		if off, ok := mkv.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	if len(validEntries) > 0 {
		// 新建临时文件
		mergeDBFile, err := NewMergeDBFile(mkv.config.DataDir)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDBFile.file.Name())

		// 重新写入有效的 entry
		for _, entry := range validEntries {
			writeOff := mergeDBFile.offset
			err := mergeDBFile.Write(entry)
			if err != nil {
				return err
			}

			// 更新索引
			mkv.indexes[string(entry.Key)] = writeOff
		}

		// 获取文件名
		dbFileName := mkv.diskFile.file.Name()
		// 关闭文件
		mkv.diskFile.file.Close()
		// 删除旧的数据文件
		os.Remove(dbFileName)

		// 获取文件名
		mergeDBFileName := mergeDBFile.file.Name()
		// 关闭文件
		mergeDBFile.file.Close()
		// 临时文件变更为新的数据文件
		os.Rename(mergeDBFileName, mkv.config.DataDir+string(os.PathSeparator)+DATA_FILE_NAME)

		mkv.diskFile = mergeDBFile
	}
	return nil
}
