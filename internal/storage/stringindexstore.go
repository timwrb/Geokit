package storage

import (
	"encoding/binary"
	"os"
	"syscall"
)

func CreateStringIndex(filename string) (*StringIndexStore, error) {
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
	binary.LittleEndian.PutUint64(s.Data[offset:], val)
	incrementHeaderOffset(s.Data, 8)
}

func (s *StringIndexStore) Get(index uint32) uint64 {
	// + 8 because of header 8 bytes, * 8 because each 'index' in the list is 8 bytes.
	offset := (index * 8) + 8
	return binary.LittleEndian.Uint64(s.Data[offset:])
}
