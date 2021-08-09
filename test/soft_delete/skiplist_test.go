package test

import (
	"math/rand"
	"strconv"
	"testing"
)

type SkipKey Key

type KeyValueTest struct {
	Key   int
	Kalue int
	SkipKey
}

func (keyValue *KeyValueTest) ToString() string {
	return strconv.Itoa(keyValue.Key)
}

func (keyValue *KeyValueTest) GetKey() int {
	return keyValue.Key
}

func (kv *KeyValueTest) Compare(keyValue interface{}) int {
	// 可能溢出
	keyValue1 := keyValue.(*KeyValueTest)
	return kv.Key - keyValue1.GetKey()
}

func TestSkipList(t *testing.T) {
	list := NewSkipList()
	for i := 0; i < 30; i++ {
		keyValue := new(KeyValueTest)
		keyValue.Key = rand.Intn(1000)
		list.AddNode(keyValue)
	}

	PrintSkipList(list)
}
