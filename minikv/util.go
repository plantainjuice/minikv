package minikv

import "encoding/binary"

func Hash(key []byte) int {
	if len(key) == 0 {
		return 0
	}
	h := 1
	for i := 0; i < len(key); i++ {
		h = (h << 5) + h + int(key[i])
	}
	return h
}

func ToBytesUint8(i uint8) []byte {
	bytes := make([]byte, 1)
	bytes[0] = byte(uint8)
	return bytes
}

func ToBytesUint32(i uint32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, i)
	return bytes
}

func ToBytesUint64(i uint64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, i)
	return bytes
}
