package test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mmmmmmmingor/minikv/core/entry"
)

func TestKeyValue(t *testing.T) {
	kv := entry.NewKeyValue([]byte("key"), []byte("value"), entry.PUT, 3)
	bytes, _ := kv.ToBytes()

	kv2 := entry.ParseFrom(bytes)
	bytes2, _ := kv2.ToBytes()

	assert.Equal(t, bytes, bytes2, "should be equal")
}
