package storage

import (
	"encoding/binary"
	"os"
	"syscall"
)

type StringIndexStore struct {
	Data []byte
	File *os.File
}

func CreateStringIndex(filename string, initFileSizeInBytes int64) (*StringIndexStore, error) {
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
	ensureHeaderInit(data, StringIndexHeaderSize)

	return &StringIndexStore{
		Data: data,
		File: f,
	}, nil
}

func (s *StringIndexStore) Close() error {
	if err := syscall.Munmap(s.Data); err != nil {
		return err
	}
	return s.File.Close()
}

func (s *StringIndexStore) Put(val uint64) {
	offset := readHeaderOffset(s.Data)
	requiredSpace := offset + 8

	if requiredSpace > uint64(len(s.Data)) {
		s.resize()
	}
	binary.LittleEndian.PutUint64(s.Data[offset:], val)
	incrementHeaderOffset(s.Data, 8)
}

func (s *StringIndexStore) Get(index uint32) uint64 {
	// + 8 because of header 8 bytes, * 8 because each 'index' in the list is 8 bytes.
	offset := (index * 8) + 8
	return binary.LittleEndian.Uint64(s.Data[offset:])
}

func (s *StringIndexStore) resize() {
	currentSize := uint64(len(s.Data))

	newSize := currentSize * 2

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
