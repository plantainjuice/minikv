package src

func Hash(key []byte)int {
	if len(key) == 0{
		return 0
	}
	h := 1
	for i := 0 ; i < len(key); i++{
		h = (h << 5) + h + int(key[i])
	}
	return h
}
