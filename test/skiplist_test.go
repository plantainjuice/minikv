package test

import (
	"math/rand"
	"testing"

	"github.com/mmmmmmmingor/minikv/core"
	"github.com/mmmmmmmingor/minikv/core/entry"
	"github.com/stretchr/testify/assert"
)

func getRandKey() []byte {
	var letters = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	key := ""
	for len := 0; len < 10; len++ {
		key += string(letters[rand.Intn(62)])
	}
	return []byte(key)
}

func TestKVSkipList(t *testing.T) {
	kv := entry.NewKeyValue([]byte("key"), []byte("value"), entry.PUT, 3)
	bytes, _ := kv.ToBytes()

	kv2 := entry.ParseFrom(bytes)
	bytes2, _ := kv2.ToBytes()
	assert.Equal(t, bytes, bytes2, "should be equal")

	list := core.NewSkipList()
	list.AddNode(&kv)
	assert.True(t, list.HasNode(&kv2) != nil, "")

	for i := 0; i < 300; i++ {
		kv := entry.NewKeyValue(getRandKey(), []byte("value"), entry.PUT, 3)
		list.AddNode(&kv)
	}
	core.PrintSkipList(list)
}
