package storage

import (
	"hash/fnv"
	"io"
	"os"
	"syscall"
)

const (
	StringHeaderSize      = 8
	StringIndexHeaderSize = 8
)

type StringStore struct {
	Data  []byte
	File  *os.File
	Index *StringIndexStore
	Hash  map[uint64][]uint32 // [hashed str => slice of str ids] - temporary map for dedup check during import
}

func CreateStringStore(filename string, indexStore *StringIndexStore, initFileSizeInBytes int64) (*StringStore, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	fileInfo, err := f.Stat()
	if err != nil {
		panic(err)
	}

	fileSize := fileInfo.Size()

	if fileSize == 0 {
		fileSize = initFileSizeInBytes
		if err := f.Truncate(fileSize); err != nil {
			panic(err)
		}
	}

	fd := int(f.Fd())
	data, err := syscall.Mmap(fd, 0, int(fileSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	// initialize the write-offset header
	ensureHeaderInit(data, StringHeaderSize)

	return &StringStore{
		Data:  data,
		File:  f,
		Index: indexStore,
		Hash:  map[uint64][]uint32{},
	}, nil
}

func (s *StringStore) Close() error {
	if err := syscall.Munmap(s.Data); err != nil {
		return err
	}
	return s.File.Close()
}

// Intern this will write or return an existing string's id/index in the mmap
func (s *StringStore) Intern(target string) uint32 {
	// early return if target already exists in store
	possiblyExistingStringId, match, stringHash := s.Lookup(target)
	if match == true {
		return possiblyExistingStringId
	}

	// Calculate based on index offset formula 8n+8
	newId := uint32((readHeaderOffset(s.Index.Data) - 8) / 8)
	startOffset := readHeaderOffset(s.Data)

	// write hash & newId into hash store,
	s.Hash[stringHash] = append(s.Hash[stringHash], newId)

	strLen := uint32(len(target))

	// String binaries get appended onto the Mmap file
	s.write(target, strLen)

	// New starting offset gets written into the Index Store
	// The newId will mathematically be the index (key) of the startOffset (value)
	s.Index.Put(startOffset)

	return newId
}

// Lookup returns id, bool if match or not, and the created hash of the string
func (s *StringStore) Lookup(target string) (uint32, bool, uint64) {
	hash := hashString(target)
	stringIds := s.Hash[hash]
	stringIdsLen := len(stringIds)

	// when s.Hash[hash] is nil, len will be 0, therefore does target not exist in the store yet
	if stringIdsLen == 0 {
		return 0, false, hash
	}

	for _, id := range stringIds {
		resolvedString := s.Get(id)
		if resolvedString == target {
			return id, true, hash
		}
	}

	return 0, false, hash
}

func (s *StringStore) write(target string, length uint32) {
	writeOffset := readHeaderOffset(s.Data)
	requiredSpace := writeOffset + uint64(length)

	if requiredSpace > uint64(len(s.Data)) {
		s.resize(requiredSpace)
	}

	copy(s.Data[writeOffset:], target)
	incrementHeaderOffset(s.Data, uint64(length))
}

func (s *StringStore) Get(targetId uint32) string {
	// Get Start offset of the string in the Index Store
	offsetStart := s.Index.Get(targetId)
	offsetEnd := s.Index.Get(targetId + 1)

	if offsetEnd == 0 {
		offsetEnd = readHeaderOffset(s.Data)
	}

	return string(s.Data[offsetStart:offsetEnd])
}

func (s *StringStore) resize(neededCapacity uint64) {
	currentSize := uint64(len(s.Data))

	newSize := currentSize * 2

	if newSize < neededCapacity {
		newSize = neededCapacity
	}

	err := syscall.Munmap(s.Data)
	if err != nil {
		panic(err)
	}

	err = s.File.Truncate(int64(newSize))
	if err != nil {
		panic(err)
	}

	fd := int(s.File.Fd())
	data, err := syscall.Mmap(fd, 0, int(newSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	s.Data = data
}

// We use a Closure here to be able to mock hashes in tests more easily
// e.g. hash collisions, when 2 distinct strings generate the same hash
// we want to assure that the store can handle this edge case
var hashString = func(s string) uint64 {
	h := fnv.New64a()
	_, _ = io.WriteString(h, s)
	return h.Sum64()
}
