package util

import (
	"github.com/OneOfOne/xxhash"
)

func Hash(bytes []byte) uint32 {
	h := xxhash.New32()
	h.Write(bytes)
	return h.Sum32()
}
