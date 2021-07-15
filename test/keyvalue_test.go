package test

import (
	"testing"

	"github.com/mmmmmmmingor/minikv"
)

func TestKeyValue(t *testing.T) {
	value := minikv.NewKeyValue(minikv.ToBytesUint32(1), minikv.ToBytesUint32(2), minikv.PUT, 3)
	bytes, _ := value.ToBytes()
	for i := 0; i < len(bytes); i++ {
		print(bytes[i])
	}
}
