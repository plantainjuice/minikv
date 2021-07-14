package test

import (
	"github.com/mmmmmmmingor/minikv/src"
	"testing"
)

func TestKeyValue(t *testing.T){
	value := src.NewKeyValue(src.ToBytesUint32(1), src.ToBytesUint32(2), src.PUT, 3)
	bytes, _ := value.ToBytes()
	for i := 0; i < len(bytes); i++{
		print(bytes[i])
	}
}
