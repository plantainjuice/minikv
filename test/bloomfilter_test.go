package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/mmmmmmmingor/minikv/util"
)

func TestBloomFilter(t *testing.T) {
	fmt.Printf("time.Now().Format(\"20060102150405\"): %v\n", time.Now().Format("20060102150405"))
	time.Sleep(time.Millisecond * 1000)
	fmt.Printf("time.Now().Format(\"20060102150405\"): %v\n", time.Now().Format("20060102150405"))

	var bf = &util.BloomFilter{K: 3, BitsPerKey: 10}

	keys := []string{"hello world", "hi", "bloom", "filter", "key", "value", "1", "value"}

	keyBytes := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		keyBytes[i] = []byte(keys[i])
	}
	bf.Generate(keyBytes)

	println(bf.Contains([]byte("hi")))

	fmt.Println("test over")
}
