package storage

import (
	"fmt"
	"testing"
)

func TestWriteOffset(t *testing.T) {
	strings, err := CreateStringStore()
	if err != nil {
		panic(err)
	}

	writeOffset := readHeaderOffset(strings.Data)
	fmt.Println(writeOffset)

	err = strings.Close()
	if err != nil {
		panic(err)
	}
}

func TestStringPut(t *testing.T) {
	strings, err := CreateStringStore()
	if err != nil {
		panic(err)
	}

	target := "addr:street"
	id := strings.Put(target)

	fmt.Println(strings.Data)

	// assert that writeoffset was increased
	// assert that hash is generated
	// assert that binaries are correctly encoded in the space at correct locations
	// assert index is updates with correct offsets

	result := strings.Get(id)

	if result != target {
		t.Fail()
	}

	err = strings.Close()
	if err != nil {
		panic(err)
	}

}
