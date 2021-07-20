package test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mmmmmmmingor/minikv"
)

func TestKeyValue(t *testing.T) {
	kv := minikv.NewKeyValue([]byte("key"), []byte("value"), minikv.PUT, 3)
	bytes, _ := kv.ToBytes()

	kv2 := minikv.ParseFrom(bytes)
	bytes2, _ := kv2.ToBytes()

	assert.Equal(t, bytes, bytes2, "should be equal")
}
