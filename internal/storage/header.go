package storage

import "encoding/binary"

func readHeaderOffset(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data[0:8])
}

func incrementHeaderOffset(data []byte, val uint64) uint64 {
	current := readHeaderOffset(data)
	newOffset := current + val
	setHeaderOffset(data, newOffset)
	return newOffset
}

func setHeaderOffset(data []byte, offset uint64) {
	binary.LittleEndian.PutUint64(data[0:8], offset)
}

func ensureHeaderInit(data []byte, startOffset uint64) {
	if binary.LittleEndian.Uint64(data[0:8]) == 0 {
		binary.LittleEndian.PutUint64(data[0:8], startOffset)
	}
}
