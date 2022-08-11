package core

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type MemStore struct {
	DataSize           uint64
	IsSnapshotFlushing int32
	Config             *Config
	flusher            *Flusher

	SkipList *SkipList
	Snapshot *SkipList

	UpdateLock *sync.RWMutex
}

func NewMemStore(config *Config, flusher *Flusher) *MemStore {
	memStore := new(MemStore)
	memStore.Config = config
	memStore.SkipList = NewSkipList(config.SkipListMaxHeight)
	memStore.flusher = flusher
	memStore.UpdateLock = &sync.RWMutex{}
	return memStore
}

func (m *MemStore) Add(kv *KeyValue) error {
	err := m.flushIfNeeded(true)
	if err != nil {
		return err
	}

	m.UpdateLock.Lock()

	if oldKv := m.SkipList.AddNode(kv); oldKv != nil {
		atomic.AddUint64(&m.DataSize, uint64(kv.GetSerializeSize()-oldKv.GetSerializeSize()))
	} else {
		atomic.AddUint64(&m.DataSize, uint64(kv.GetSerializeSize()))
	}

	m.UpdateLock.Unlock()
	err = m.flushIfNeeded(false)
	if err != nil {
		return err
	}

	return nil
}

func (m *MemStore) flushIfNeeded(shouldBlocking bool) error {
	if m.DataSize > uint64(m.Config.MaxMemstoreSize) {
		//todo 万一请求太多这里直接报错吗？
		if m.IsSnapshotFlushing == 1 && shouldBlocking {
			return fmt.Errorf(`memstore is full, currentDataSize= %d B, maxMemstoreSize= %d B,	
					 please wait until the flushing is finished`, m.DataSize, m.Config.MaxMemstoreSize)
		} else if atomic.CompareAndSwapInt32(&m.IsSnapshotFlushing, m.IsSnapshotFlushing, 1) {
			go flusherTask(m)
		}
	}
	return nil
}

func flusherTask(m *MemStore) {
	m.UpdateLock.Lock()
	m.Snapshot = m.SkipList
	// TODO MemStoreIter may find the kvMap changed ? should synchronize ?
	m.SkipList = NewSkipList(m.Config.SkipListMaxHeight)
	m.DataSize = 0
	m.UpdateLock.Unlock()

	success := false
	for i := 0; i < m.Config.FlushMaxRetries; i++ {
		if m.flusher.Flush(m.Snapshot) != nil {
			success = true
			break
		}
	}

	if success {
		m.Snapshot = nil
		atomic.CompareAndSwapInt32(&m.IsSnapshotFlushing, m.IsSnapshotFlushing, 0)
	}
}

func (m *MemStore) Get(key []byte) *KeyValue {
	m.UpdateLock.RLock()
	defer m.UpdateLock.Unlock()
	return m.SkipList.HasNode(key).KV
}

func (m *MemStore) Close() {
	flusherTask(m)
	m.flusher.Wait()
}

func (m *MemStore) CreateIterator() <-chan *KeyValue {
	m.UpdateLock.RLock()
	c := make(chan *KeyValue)
	go func() {
		for i := range m.SkipList.Iterator() {
			c <- i
		}
		close(c)
		m.UpdateLock.RUnlock()
	}()
	return c
}
