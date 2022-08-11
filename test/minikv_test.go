package test

import (
	"sync"
	"testing"

	"github.com/mmmmmmmingor/minikv/core"
	"github.com/mmmmmmmingor/minikv/util"
	"github.com/stretchr/testify/assert"
)

func TestPut(t *testing.T) {
	conf := &core.Config{
		DataDir:         "./minikv",
		MaxMemstoreSize: 2 * 1024 * 1024,
		FlushMaxRetries: 1,
		MaxDiskFiles:    10,
	}

	db := core.NewMiniKv(conf)

	db.Open()

	var wg sync.WaitGroup
	var totalKVSize int32 = 100
	var routineNum int32 = 5

	var i int32 = 0
	for ; i < routineNum; i++ {
		wg.Add(1)

		size := totalKVSize / routineNum

		go func(db *core.MiniKv, start, end int32) {
			defer wg.Done()
			println("start:", start, "end:", end)
			for i := start; i < end; i++ {
				db.Put(util.Int32ToBytes(i), util.Int32ToBytes(i))
			}
		}(db, i*size, (i+1)*size-1)
	}

	wg.Wait()

	db.Close()

	println("success")

	// TODO
	// kv = db.Scan()
	// var current = 0

	// for
}

func TestMixedOp(t *testing.T) {
	conf := &core.Config{
		DataDir:         "./minikv",
		MaxMemstoreSize: 2 * 1024 * 1024,
	}

	db := core.NewMiniKv(conf)

	A := []byte("A")
	B := []byte("B")
	C := []byte("C")

	db.Put(A, A)
	assert.Equal(t, A, db.Get(A).GetValue())

	db.Delete(A)
	assert.Nil(t, db.Get(A))

	db.Put(A, B)
	assert.Equal(t, db.Get(A).GetValue(), B)

	db.Put(B, A)
	assert.Equal(t, db.Get(B).GetValue(), A)

	db.Put(B, B)
	assert.Equal(t, db.Get(B).GetValue(), B)

	db.Put(C, C)
	assert.Equal(t, db.Get(C).GetValue(), C)

	db.Delete(B)
	assert.Nil(t, db.Get(B))
}

func TestScanIter(t *testing.T) {
	// TODO
}
