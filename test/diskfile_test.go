package test

import (
	"testing"

	"github.com/mmmmmmmingor/minikv/diskfile"
	minikv "github.com/mmmmmmmingor/minikv/keyvalue"
)

func TestBlockMeta(t *testing.T) {
	kv := minikv.NewKeyValue([]byte("key"), []byte("value"), minikv.PUT, 3)
	bloomFilter := []byte("bloomFilter")

	diskfile.NewBlockMeta(kv, 0, 0, bloomFilter)

}
