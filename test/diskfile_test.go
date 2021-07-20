package test

import (
	"testing"

	"github.com/mmmmmmmingor/minikv"
	"github.com/mmmmmmmingor/minikv/diskfile"
)

func TestBlockMeta(t *testing.T) {
	kv := minikv.NewKeyValue([]byte("key"), []byte("value"), minikv.PUT, 3)
	bloomFilter := []byte("bloomFilter")

	diskfile.New(kv, 0, 0, bloomFilter)

}
