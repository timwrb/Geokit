package storage

import (
	"path/filepath"
	"testing"
)

func TestStringIndexStore(t *testing.T) {

	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_string_index_store.bin")

	indexStore, err := CreateStringIndex(testFilePath, 1024)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		err := indexStore.Close()
		if err != nil {
			t.Logf("Error closing store: %v", err)
		}
	})

	testId := uint64(694)

	headerOffsetBeforeInsert := readHeaderOffset(indexStore.Data)
	if headerOffsetBeforeInsert != 8 {
		t.Log("Header Offset is not 8 BETWEEN init AND insert of first item")
		t.Fail()
	}

	indexStore.Put(testId)

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
}

func TestStringIndexStoreResizing(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_string_index_store_resizing.bin")

	indexStore, err := CreateStringIndex(testFilePath, 16)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		err := indexStore.Close()
		if err != nil {
			t.Logf("Error closing store: %v", err)
		}
	})

	var targetA uint64 = 1234
	var targetB uint64 = 5678

	indexStore.Put(targetA)

	retrievedTargetA := indexStore.Get(0)
	if targetA != retrievedTargetA {
		t.Fatalf("offset value retrieval from index store was not successful %v", retrievedTargetA)
	}

	bufferSize := len(indexStore.Data)

	if bufferSize != 16 {
		t.Fatalf("buffer size set in index store init is not acutal length %v", bufferSize)
	}

	indexStore.Put(targetB)

	bufferSize = len(indexStore.Data)
	if bufferSize != 32 {
		t.Fatalf("buffer size did not double after writing a record to insufficient space, expected 32, got: %v", bufferSize)
	}

	retrievedTargetB := indexStore.Get(1)
	if targetB != retrievedTargetB {
		t.Fatalf("offset value retrieval of targetB (after resize) from index store was not successful, expected 5678, got: %v", retrievedTargetB)
	}

}
