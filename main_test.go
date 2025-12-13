package main

import (
	"Geokit/internal/storage"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestStringSerialization(t *testing.T) {
	target := "addr:street"

	// first 8 bytes reserved for persisting the offset for writing new data.
	// first read offset is at 8
	buf := make([]byte, 1024)

	offset := 0
	strLen := uint32(len(target))

	// write the str length encoded in 4 bytes to the start of the buf
	binary.LittleEndian.PutUint32(buf[offset:], strLen)

	// jump over the strLen declaration in the buf to write the actual string binary in.
	offset += 4

	// now we copy the string (that is already binary in memory) in the buf positioned after its length definition
	// string is automatically written inside in binary, so automatically encoded
	copy(buf[offset:], target)

	// ----- READ -----

	// start at beginning of buf
	readOffset := 0

	// get decoded len of first written string als unit32
	//decodedLen := binary.LittleEndian.Uint32(buf[readOffset:])

	// jump over str length declaration (4 bytes, the whole 32bit int)
	readOffset += 4

	// we access the buf and read from start of readOffset to the end of readOffset + decoded str length
	// so we read the whole binary declaration of the first written string in the buf
	decodedStr := storage.DecodeStringFromRecordOffset(0, buf)

	fmt.Println("Original:", target)

	t.Logf("Raw String Bytes %v", buf[:20])
	t.Logf("Decoded String: %v", decodedStr)

	//t.Logf("Geschrieben: %d Bytes. Buffer Vorschau: %v", offset, buf[:20])
}
