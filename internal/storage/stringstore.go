package storage

import (
	"encoding/binary"
	"os"
	"syscall"
)

type StringStoreRecord struct {
	id  uint32
	str string
}

type StringStore struct {
	Data []byte
	File *os.File
}

type LookupIndex struct {
	id     uint32
	offset int
}

func CreateStringStore() *StringStore {
	f, err := os.OpenFile("test.dat", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	f.Truncate(1024)

	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			panic(err)
		}
	}(f)

	fd := int(f.Fd())

	data, err := syscall.Mmap(
		fd,
		0,
		1024,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		panic(err)
	}

	defer func(b []byte) {
		err := syscall.Munmap(b)
		if err != nil {
			panic(err)
		}
	}(data)

	// we reserve the first 8 bytes for storing current offset.
	currentOffseet := 8

	return &StringStore{data, f}

	// 5. Daten manipulieren (als Bytes)
	// Hier schreiben wir "Go" an den Anfang
	//copy(data, []byte("Go is fast"))

	// 6. Der "Unsafe" Cast (Das wolltest du lernen)
	// Hier tun wir so, als wären die Bytes ein int32 (oder dein NodeStruct)
	// Achtung: Das hier ist Pointer-Arithmetic.
	//ptr := unsafe.Pointer(&data[0])

	// Sagen wir, die ersten 4 Bytes sind ein Int32
	//intVal := (*int32)(ptr)
	//*intVal = 99999 // Wir schreiben direkt in den Speicher/Datei

	//fmt.Println("Daten geschrieben via Mmap!")
}

func PutString(target *string, buf []byte) uint32 {
	offset := getCurrentStartingOffset(buf)
	strLen := uint32(len(*target))
	// write the str length encoded in 4 bytes to the start of the buf
	binary.LittleEndian.PutUint32(buf[offset:], strLen)

	return 1
}

func putStringTest(target *string, store *StringStore) {

	// first we have to get the offset for the first byte that is 'free' so empty.
	// maybe there is a method that declares the length of the used space in the buf/file, would be ideal
	// otherwise we have to iterate & jump linear
	//offset := 0

	// get teh 32bit length of the target, so 4 byte as a prefix for the store entry.
	//strLen := uint32(len(target))

	// Structure of each 'record' in StringStore
	// Str Length uint32	 	Unique ID uint32	value of str len in bytes, individual
	// [] [] [] []				[] [] [] []			strLen * []

	return 1
}

// StringStoreRecordHeaderSizeInBytes 4 = only StrLen, 8 = StrLen + id
const StringStoreRecordHeaderSizeInBytes = 4

// BufHeaderSizeInBytes header stores the current offset, so where data ends.
const BufHeaderSizeInBytes = 8

func getCurrentStartingOffset(buf []byte) int {
	// todo: read header from buf and return that decoded uint32
	return BufHeaderSizeInBytes
}
func nextOffset(currentOffset int, buf []byte) int {
	strLen := binary.LittleEndian.Uint32(buf[currentOffset : currentOffset+4])
	return currentOffset + StringStoreRecordHeaderSizeInBytes + int(strLen)
}

func GetOffsetForId(Id int, lookupStore *[]LookupIndex) int {
	// TODO
	// look in the index after id and return the offset
	return 8
}

func GetStringById(id uint32, buf []byte) string {
	// todo: check lookup table to
	recordOffset := int(BufHeaderSizeInBytes + 0)
	return DecodeStringFromRecordOffset(recordOffset, buf)
}

func DecodeStringFromRecordOffset(recordOffset int, buf []byte) string {
	decodedStrLen := binary.LittleEndian.Uint32(buf[recordOffset:])
	strStartOffset := recordOffset + StringStoreRecordHeaderSizeInBytes
	rawStringBytes := buf[strStartOffset : strStartOffset+int(decodedStrLen)]
	decodedStr := string(rawStringBytes)

	return decodedStr
}

/*
Index lookup table bauen mit id und offset
erste 8 bytes vom buff reservieren für offset zum schreiben
method bauen zum auslesen des aktuellen offsets ab dem die daten leer sind (wo neuer record reinkommt)
update method bauen wo einfach nur geupdated wird die länge und eine str length geparsed wird, header wird von der const hinzufegügt
dann write funktion und read funktion
*/
