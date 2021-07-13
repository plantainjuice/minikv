package test

import (
	"fmt"
	"testing"

	"github.com/mmmmmmmingor/minikv/minikv"
)

func TestBloomFilter(t *testing.T) {

	var bf = &minikv.BloomFilter{K: 3, BitsPerKey: 10}

	keys := []string{"hello world", "hi", "bloom", "filter", "key", "value", "1", "value"}

	keyBytes := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		keyBytes[i] = []byte(keys[i])
	}
	bf.Generate(keyBytes)

	println(bf.Contains([]byte("hi")))

	fmt.Println("test over")

}
