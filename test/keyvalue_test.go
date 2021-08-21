package test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mmmmmmmingor/minikv/core"
)

func TestKeyValue(t *testing.T) {
	kv := core.NewKeyValue([]byte("key"), []byte("value"), core.PUT, 3)
	bytes, _ := kv.ToBytes()

	kv2 := core.ParseFrom1(bytes)
	bytes2, _ := kv2.ToBytes()

	assert.Equal(t, bytes, bytes2, "should be equal")
}
