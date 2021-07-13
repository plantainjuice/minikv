package minikv

import (
	"errors"
	"log"
)

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

func NewKeyValue(key, value []byte, op Op, sequenceId uint64) {
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
	return len(kv.key) + OP_SIZE + SEQ_ID_SIZE
}

func (kv KeyValue) GetSerializeSize() uint32 {
	return RAW_KEY_LEN_SIZE + VAL_LEN_SIZE + kv.GetRawKeyLen() + len(kv.value)
}

func (kv KeyValue) ToBytes() ([]byte, error) {
	rawKeyLen := kv.GetRawKeyLen()
	pos := 0
	bytes := make([]byte, kv.GetSerializeSize())

	// Encode raw key length
	rawKeyLenBytes := util.ToBytesUint32(rawKeyLen)
	bytes[pos:RAW_KEY_LEN_SIZE] = rawKeyLenBytes
	pos += RAW_KEY_LEN_SIZE

	// Encode value length
	valLenBytes := util.ToBytesUint32(len(kv.value))
	bytes[pos:VAL_LEN_SIZE] = valLenBytes
	pos += VAL_LEN_SIZE

	// Encode key
	bytes[pos:len(kv.key)] = kv.key
	pos += len(kv.key)

	// Encode Op
	bytes[pos : pos+1] = util.ToBytesUint8(kv.op)
	pos += 1

	// Encode sequenceId
	seqIdBytes := util.ToBytesUint64(kv.sequenceId)
	bytes[pos:SEQ_ID_SIZE] = seqIdBytes
	pos += SEQ_ID_SIZE

	// Encode value
	bytes[pos:len(kv.value)] = kv.value
	return bytes, nil
}
