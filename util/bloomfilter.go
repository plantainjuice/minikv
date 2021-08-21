package util

type BloomFilter struct {
	K          int
	BitsPerKey int
	bitLen     int
	result     []byte
}

func NewBloomFilter(k, bitsPerKey int) *BloomFilter {
	return &BloomFilter{
		K:          k,
		BitsPerKey: bitsPerKey,
	}
}

func (bf *BloomFilter) Generate(keys [][]byte) []byte {

	bf.bitLen = len(keys) * bf.BitsPerKey

	// align the bitLen.
	bf.bitLen = ((bf.bitLen + 7) / 8) << 3
	if bf.bitLen < 64 {
		bf.bitLen = 64
	}

	bf.result = make([]byte, bf.bitLen>>3)
	for i := 0; i < len(keys); i++ {
		h := Hash(keys[i])
		for t := 0; t < bf.K; t++ {
			idx := (h%bf.bitLen + bf.bitLen) % bf.bitLen // 获取索引位
			bf.result[idx/8] |= 1 << (idx % 8)           
			delta := (h >> 17) | (h << 15)
			h += delta
		}
	}
	return bf.result
}

func (bf *BloomFilter) Contains(key []byte) bool {
	h := Hash(key)
	for t := 0; t < bf.K; t++ {
		idx := (h%bf.bitLen + bf.bitLen) % bf.bitLen
		if (bf.result[idx/8] & (1 << (idx % 8))) == 0 {
			return false
		}
		delta := (h >> 17) | (h << 15)
		h += delta
	}
	return true
}
