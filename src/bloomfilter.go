package bloomfilter

import "fmt"

type BloomFilter struct {
	K          int
	BitsPerKey int
	BitLen     int
	Result     []byte
}

func (bf* BloomFilter) Generate(keys [][]byte) []byte {

	bitLen := len(keys) * bf.BitsPerKey

	// align the bitLen.
	bitLen = ((bitLen + 7) / 8) << 3
	if bitLen < 64 {
		bitLen = 64
	} 

	bf.Result = make([]byte, (bitLen >> 3))

	for i := range keys {
		fmt.Println(len(keys[i]))
	}

	return nil
}

func Contains(key []byte) bool {
	return false
}
