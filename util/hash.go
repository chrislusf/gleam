package util

import (
	"hash/fnv"
)

func Hash(bytes []byte) uint32 {
	h := fnv.New32a()
	h.Write(bytes)
	return h.Sum32()
}
