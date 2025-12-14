package storage

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"syscall"
)

const (
	StringHeaderSize      = 8
	StringIndexHeaderSize = 8
)

type StringStoreRecord struct {
	id  uint32
	str string
}

type StringStore struct {
	Data  []byte
	File  *os.File
	Index StringIndexStore
	Hash  map[uint64][]uint32 // [hashed str => slice of str ids] - temporary map for dedup check during import
}

type StringIndexStore struct {
	Data []byte
	File *os.File
}

func CreateStringStore(filename string) (*StringStore, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	err = f.Truncate(1024)
	if err != nil {
		panic(err)
	}

	fd := int(f.Fd())
	data, err := syscall.Mmap(fd, 0, 1024, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	// initialize the write-offset header
	ensureHeaderInit(data, StringHeaderSize)

	index, err := CreateStringIndex("string_indexes.bin")
	if err != nil {
		panic(err)
	}

	return &StringStore{
		Data:  data,
		File:  f,
		Index: *index,
		Hash:  map[uint64][]uint32{},
	}, nil
}

func (s *StringStore) Close() error {
	if err := syscall.Munmap(s.Data); err != nil {
		return err
	}
	return s.File.Close()
}

func (s *StringStore) Put(target string) uint32 {
	startOffset := readHeaderOffset(s.Data)
	// 1. hash string
	hash := hashString(target)
	// 2. check if hash exists: if yes, 3.a if no, 3.b
	if s.Hash[hash] != nil {
		// if the hash already exists
		fmt.Println("!!! Duplicate hash found, not supported yet.")

		// ------------------------------------------------------
		// s.Hash ist deine map[uint64][]uint32
		// hash ist dein uint64 Key
		// newID ist die uint32 ID, die du gerade generiert hast
		//s.Hash[hash] = append(s.Hash[hash], newID)
		// ------------------------------------------------------

		// next: get value of hash key, iterate through the ids to get the string binaries and decode them.
		// loop through, check if a string matches an existing one with strict equality
		// if so, return its id.
		// if no strings match, continue writing that string, but add the id to that exact hashes slice.
	}
	s.write(target)
	// append offset to index slice
	binary.LittleEndian.PutUint64(s.Index.Data, startOffset)
	// 7. take that write offset and put it in indexes slice
	// 8. return id of str reccord

	return 12
}

func (s *StringStore) write(target string) {
	writeOffset := readHeaderOffset(s.Data)
	strLen := uint32(len(target))

	// 5. write str binary (copy) at write offset pos
	copy(s.Data[writeOffset:], target)
	// 6. increment write offset by strlen in buf header
	incrementHeaderOffset(s.Data, uint64(strLen))

}

func (s *StringStore) Get(id uint32) string {
	// 1. nimm index[id] und hole offset
	// 2. check if last item in index, wenn nein, -> a, wenn ja, -> b
	// 3.a increase die id+1 und hole dir diesen offset, dann subtrahieren und wir haben die str len
	// 3.b subtract id offset from store header write offset to get str len
	// 4. decode string binaries and return

	return "test"
}

func hashString(s string) uint64 {
	h := fnv.New64a()
	_, _ = io.WriteString(h, s)
	return h.Sum64()
}

/*
func DecodeStringFromRecordOffset(recordOffset int, buf []byte) string {
	decodedStrLen := binary.LittleEndian.Uint32(buf[recordOffset:])
	strStartOffset := recordOffset + StringStoreRecordHeaderSizeInBytes
	rawStringBytes := buf[strStartOffset : strStartOffset+int(decodedStrLen)]
	decodedStr := string(rawStringBytes)

	return decodedStr
}*/
