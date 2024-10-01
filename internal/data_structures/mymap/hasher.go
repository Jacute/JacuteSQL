package mymap

func djb2(key string) uint32 {
	hash := uint32(5381)
	for _, c := range key {
		char_code := uint32(c)

		hash = ((hash << 5) + hash) + char_code
	}
	return hash
}
