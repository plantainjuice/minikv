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

	go mkv.compactor.Compact()

	return mkv
}

func (mkv MiniKv) Close() {
	mkv.memStore.Close()
	mkv.diskStore.Close()
	mkv.compactor.StopRunning()
}

func (mkv *MiniKv) Put(key, value []byte) error {
	kv := NewKeyValue(key, value, PUT, atomic.AddUint64(&mkv.sequenceId, 1))
	return mkv.memStore.Add(&kv)
}

func (mkv *MiniKv) Delete(key []byte) error {
	kv := NewKeyValue(key, []byte{}, DELETE, atomic.AddUint64(&mkv.sequenceId, 1))
	return mkv.memStore.Add(&kv)
}

func (mkv MiniKv) Get(key []byte) *KeyValue {
	// mkv.Scan(key, []byte{})

	return nil
}

func (mkv MiniKv) Scan0() {
	// TODO
}

func (mkv MiniKv) Scan(start, stop []byte) {
	// TODO
}
