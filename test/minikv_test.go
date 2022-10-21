package test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/mmmmmmmingor/minikv/core"
)

func TestPut(t *testing.T) {
	conf := &core.Config{
		DataDir:         "./minikv",
		MaxMemstoreSize: 2 * 1024 * 1024,
		FlushMaxRetries: 1,
	}

	db, err := core.Open(conf)
	defer db.Close()
	if err != nil {
		t.Error(err)
	}

	var wg sync.WaitGroup
	totalKVSize := 100
	routineNum := 5
	keyPrefix := "test_key_"
	valPrefix := "test_val_"

	for i := 0; i < routineNum; i++ {
		wg.Add(1)

		size := totalKVSize / routineNum

		go func(db *core.MiniKv, start, end int) {
			defer wg.Done()

			for i := start; i < end; i++ {
				err := db.Put([]byte(keyPrefix+strconv.Itoa(i)), []byte(valPrefix+strconv.Itoa(i)))
				if err != nil {
					t.Error(err)
				}
			}
		}(db, i*size, (i+1)*size-1)
	}

	wg.Wait()
}

func TestGet(t *testing.T) {
	conf := &core.Config{
		DataDir:         "./minikv",
		MaxMemstoreSize: 2 * 1024 * 1024,
		FlushMaxRetries: 1,
	}

	db, err := core.Open(conf)
	defer db.Close()
	if err != nil {
		t.Error(err)
	}

}

func TestMixedOp(t *testing.T) {
	conf := &core.Config{
		DataDir:         "./minikv",
		MaxMemstoreSize: 2 * 1024 * 1024,
	}

	db, err := core.Open(conf)
	defer db.Close()
	if err != nil {
		t.Error(err)
	}
}
