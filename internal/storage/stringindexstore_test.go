package storage

import (
	"testing"
)

func TestStringIndexStore(t *testing.T) {
	indexStore, err := CreateStringIndex()
	if err != nil {
		panic(err)
	}

	testId := 694

	headerOffsetBeforeInsert := readHeaderOffset(indexStore.Data)
	if headerOffsetBeforeInsert != 8 {
		t.Log("Header Offset is not 8 BETWEEN init AND insert of first item")
		t.Fail()
	}

	indexStore.Put(uint64(testId))

	headerOffsetAfterInsert := readHeaderOffset(indexStore.Data)
	if headerOffsetAfterInsert != 16 {
		t.Log("Header Offset is not 16 AFTER insert of first item")
		t.Fail()
	}

	indexStore.Put(uint64(2246))
	indexStore.Put(uint64(8531))

	thirdOffset := indexStore.Get(2) // get 3rd item, so the "8531"

	if thirdOffset != uint64(8531) {
		t.Log("Storage retrieval by index/id does not work.")
		t.Fail()
	}

	headerOffsetAtEnd := readHeaderOffset(indexStore.Data)
	if headerOffsetAtEnd != 32 {
		t.Log("Header Offset is not 32 AFTER insert of third item")
		t.Fail()
	}

	err = indexStore.Close()
	if err != nil {
		panic(err)
	}

	//fmt.Println(thirdOffset)
	//fmt.Println(indexStore.Data)
}
