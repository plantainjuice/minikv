package core

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/mmmmmmmingor/minikv/core/entry"
	"github.com/mmmmmmmingor/minikv/diskstore"
)

type MemStore struct {
	DataSize uint64
	Config   *Config
	flusher  *diskstore.Flusher

	SkipList *SkipList
	Snapshot *SkipList

	UpdateLock         *sync.RWMutex
	IsSnapshotFlushing int32
}

func NewMemStore(config *Config, flusher *diskstore.Flusher) *MemStore {
	memStore := new(MemStore)
	memStore.Config = config
	memStore.SkipList = NewSkipList(config.LevelDBMaxHeight)
	memStore.flusher = flusher
	return memStore
}

func (m *MemStore) Add(kv *entry.KeyValue) {
	m.flushIfNeeded(true)
	m.UpdateLock.RLock()
	m.SkipList.AddNode(kv)
	atomic.AddUint64(&m.DataSize, uint64(kv.GetSerializeSize()))
	m.UpdateLock.RUnlock()
	m.flushIfNeeded(false)
}

func (m *MemStore) flushIfNeeded(shouldBlocking bool) error {
	if m.DataSize > uint64(m.Config.MaxMemstoreSize) {
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
	m.SkipList = NewSkipList(m.Config.LevelDBMaxHeight)
	m.DataSize = 0
	m.UpdateLock.Unlock()

	success := false
	for i := 0; i < m.Config.FlushMaxRetries; i++ {
		//TODO here
	}

	if success {
		// TODO MemStoreIter may get a NPE because we set null here ? should synchronize ?
		m.Snapshot = nil
		atomic.CompareAndSwapInt32(&m.IsSnapshotFlushing, m.IsSnapshotFlushing, 0)
	}
}
