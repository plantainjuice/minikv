package core

import (
	"log"
	"sync/atomic"
)

type MiniKv struct {
	memStore   *MemStore
	diskStore  *DiskStore
	compactor  *Compactor
	sequenceId uint64
	config     *Config
}

func NewMiniKv(config *Config) *MiniKv {
	return &MiniKv{
		config: config,
	}
}

func (mkv *MiniKv) Open() *MiniKv {
	if mkv.config == nil {
		log.Fatal("config can not be none")
	}

	conf := mkv.config
	mkv.diskStore = NewDiskStore(conf.DataDir, conf.MaxDiskFiles)
	mkv.diskStore.Open()

	mkv.sequenceId = 0

	mkv.memStore = NewMemStore(conf, NewFlusher(mkv.diskStore))

	mkv.compactor = NewCompactor(mkv.diskStore)

	return mkv
}

func (mkv MiniKv) Close() {
	mkv.diskStore.Close()
}

func (mkv *MiniKv) Put(key, value []byte) {
	kv := NewKeyValue(key, value, PUT, atomic.AddUint64(&mkv.sequenceId, 1))
	mkv.memStore.Add(&kv)
}

func (mkv *MiniKv) Delete(key, value []byte) {
	kv := NewKeyValue(key, value, DELETE, atomic.AddUint64(&mkv.sequenceId, 1))
	mkv.memStore.Add(&kv)
}

func (mkv MiniKv) Get(key []byte) *KeyValue {
	// TODO
	return nil
}

func (mkv MiniKv) Scan(start, stop []byte) {
	// TODO
}
