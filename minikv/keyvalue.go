package minikv

import (
	"errors"
	"log"
)


//todo 改为动态创建解析
const (
	RAW_KEY_LEN_SIZE = 4
	VAL_LEN_SIZE     = 4
	OP_SIZE          = 1
	SEQ_ID_SIZE      = 8
)

type KeyValue struct {
	key        []byte
	value      []byte
	op         Op
	sequenceId uint64
}

type Op = uint8

const (
	PUT    Op = 0
	DELETE Op = 1
)

func NewKeyValue(key, value []byte, op Op, sequenceId uint64) KeyValue{
	if len(key) == 0 || len(value) == 0 || op < 0 || sequenceId < 0 {
		log.Fatal(errors.New("NewKeyValue param invalid"))
	}
	return KeyValue{
		key:        key,
		value:      value,
		op:         op,
		sequenceId: sequenceId,
	}
}

func (kv KeyValue) GetKey() []byte {
	return kv.key
}

func (kv KeyValue) GetValue() []byte {
	return kv.value
}

func (kv KeyValue) GetOp() Op {
	return kv.op
}

func (kv KeyValue) GetSequenceId() uint64 {
	return kv.sequenceId
}

func (kv KeyValue) GetRawKeyLen() uint32 {
	return uint32(len(kv.key) + OP_SIZE + SEQ_ID_SIZE)
}

func (kv KeyValue) GetSerializeSize() uint32 {
	return RAW_KEY_LEN_SIZE + uint32(VAL_LEN_SIZE) + kv.GetRawKeyLen() + uint32(len(kv.value))
}

func (kv KeyValue) ToBytes() ([]byte, error) {
	rawKeyLen := kv.GetRawKeyLen()
	pos := 0
	bytes := make([]byte, kv.GetSerializeSize())

	// Encode raw key length
	rawKeyLenBytes := ToBytesUint32(rawKeyLen)
	for i := 0; pos < RAW_KEY_LEN_SIZE; pos++{
		bytes[pos] = rawKeyLenBytes[i]
		i++
	}

	// Encode value length
	valLenBytes := ToBytesUint32(uint32(len(kv.value)))
	bytes = append(bytes,valLenBytes[:]...)

	// Encode key
	bytes = append(bytes,kv.key...)

	// Encode Op
	bytes = append(bytes,ToBytesUint8(kv.op)...)

	// Encode sequenceId
	bytes = append(bytes,ToBytesUint64(kv.sequenceId)...)

	// Encode value
	bytes = append(bytes,kv.value...)

	return bytes, nil
}
