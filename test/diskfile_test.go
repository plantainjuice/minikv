package test

import (
	"testing"

	"github.com/mmmmmmmingor/minikv/core/entry"
	"github.com/mmmmmmmingor/minikv/diskfile"
)

func TestBlockMeta(t *testing.T) {
	kv := entry.NewKeyValue([]byte("key"), []byte("value"), entry.PUT, 3)
	bloomFilter := []byte("bloomFilter")

	diskfile.NewBlockMeta(kv, 0, 0, bloomFilter)

}
