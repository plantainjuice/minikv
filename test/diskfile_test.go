package test

import (
	"testing"

	"github.com/mmmmmmmingor/minikv/core"
)

func TestBlockMeta(t *testing.T) {
	kv := core.NewKeyValue([]byte("key"), []byte("value"), core.PUT, 3)
	bloomFilter := []byte("bloomFilter")

	core.NewBlockMeta(kv, 0, 0, bloomFilter)

}
