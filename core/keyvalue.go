package core

import (
	"errors"
	"log"
	"strconv"

	"github.com/mmmmmmmingor/minikv/util"
)

/*	keyValue struct
 * ┌───────────┬──────────┬───────────────────────┬──┬──────────┬──────────────────────────────────────┐
 * │ rawKeyLen │ valueLen │        key            │op│sequenceId│             value                    │
 * └───────────┴──────────┴───────────────────────┴──┴──────────┴──────────────────────────────────────┘
 * |----4------|----4-----|---------- var --------| 1|-----8----|-------------- var -------------------|
 * |-----------8----------|--------------- rawKeyLen -----------|-------------- valueLen --------------|
 */

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

func NewKeyValue(key, value []byte, op Op, sequenceId uint64) KeyValue {
	if len(key) == 0 || len(value) == 0 {
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

func (kv KeyValue) ToString() string {
	return string(kv.key) + string(kv.value) + string(kv.op) + strconv.FormatUint(kv.sequenceId, 10)
}

func (kv KeyValue) ToBytes() ([]byte, error) {
	rawKeyLen := kv.GetRawKeyLen()
	pos := 0
	bytes := make([]byte, kv.GetSerializeSize())

	// Encode raw key length
	buf := util.Uint32ToBytes(rawKeyLen)
	copy(bytes[pos:pos+4], buf)
	pos += 4

	// Encode value length
	buf = util.Uint32ToBytes(uint32(len(kv.value)))
	copy(bytes[pos:pos+4], buf)
	pos += 4

	// Encode key
	copy(bytes[pos:pos+len(kv.key)], kv.key)
	pos += len(kv.key)

	// Encode Op
	buf = util.Uint8ToBytes(kv.op)
	copy(bytes[pos:pos+1], buf)
	pos += 1

	// Encode sequenceId
	buf = util.Uint64ToBytes(kv.sequenceId)
	copy(bytes[pos:pos+8], buf)
	pos += 8

	// Encode value
	copy(bytes[pos:pos+len(kv.value)], kv.value)
	pos += len(kv.value)

	return bytes, nil
}

func ParseFrom1(bytes []byte) KeyValue {
	if RAW_KEY_LEN_SIZE+VAL_LEN_SIZE >= len(bytes) {
		log.Fatalln("Invalid len. len: " + strconv.Itoa(len(bytes)))
	}
	// Decode raw key length
	pos := 0
	rawKeyLen := util.BytesToUint32(bytes[pos : pos+RAW_KEY_LEN_SIZE])
	pos += RAW_KEY_LEN_SIZE

	// Decode value length
	valLen := util.BytesToUint32(bytes[pos : pos+VAL_LEN_SIZE])
	pos += VAL_LEN_SIZE

	// Decode key
	keyLen := rawKeyLen - OP_SIZE - SEQ_ID_SIZE
	key := bytes[pos : pos+int(keyLen)]
	pos += int(keyLen)

	// Decode Op
	op := Op(bytes[pos])
	pos += 1

	// Decode sequenceId
	sequenceId := util.BytesToUint64(bytes[pos : pos+SEQ_ID_SIZE])
	pos += SEQ_ID_SIZE

	// Decode value
	val := bytes[pos : pos+int(valLen)]
	return NewKeyValue(key, val, op, sequenceId)
}
