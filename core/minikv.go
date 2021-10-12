package core

import (
	"log"
	"sync/atomic"
)

type MiniKv struct {
	sequenceId uint64
	memStore   *MemStore
	diskStore  *DiskStore
	compactor  *Compactor
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

func (mkv *MiniKv) Delete(key []byte) {
	kv := NewKeyValue(key, []byte{}, DELETE, atomic.AddUint64(&mkv.sequenceId, 1))
	mkv.memStore.Add(&kv)
}

func (mkv MiniKv) Get(key []byte) *KeyValue {
	return mkv.memStore.Get(key)
}

func (mkv MiniKv) Scan0() {
	// TODO
}

func (mkv MiniKv) Scan(start, stop []byte) {
	// TODO
}
