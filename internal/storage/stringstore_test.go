package storage

import (
	"path/filepath"
	"testing"
)

func TestWriteOffset(t *testing.T) {

	tmpDir := t.TempDir()

	stringStorePath := filepath.Join(tmpDir, "test_string_store.bin")
	stringIndexStorePath := filepath.Join(tmpDir, "test_string_index_store.bin")

	indexStore, err := CreateStringIndex(stringIndexStorePath, 1024)
	if err != nil {
		panic(err)
	}

	stringStore, err := CreateStringStore(stringStorePath, indexStore, 1024)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		_ = indexStore.Close()
		_ = stringStore.Close()
	})

	writeOffset := readHeaderOffset(stringStore.Data)
	if writeOffset != 8 {
		t.Log("Write offset after init of string store is not 8")
		t.Fail()
	}

	target := "addr:street"

	stringIdOne := stringStore.Intern(target)
	stringIdTwo := stringStore.Intern(target)

	if stringIdOne != stringIdTwo {
		t.Log("identical String was written twice in the store")
		t.Fail()
	}

	writeOffset = readHeaderOffset(stringStore.Data)
	if writeOffset != 19 {
		t.Log("Write offset after first insert did not increase")
		t.Fail()
	}

	firstIndexStoreRecord := stringStore.Index.Get(0)
	if firstIndexStoreRecord != 8 {
		// first index (0) must be 8 here, since first entry starts at offset 8 in string store
		t.Log("Index Store offset after first insert is not 8")
		t.Fail()
	}

	if len(stringStore.Hash) != 1 {
		t.Log("String Hash list has no new entry. ")
		t.Fail()
	}

	targetCheck := stringStore.Get(0)
	if targetCheck != target {
		t.Log("Retrieved target string does not equal initially written string")
		t.Fail()
	}

	targetTwo := "addr:postcode"

	stringIdThree := stringStore.Intern(targetTwo)

	if stringIdThree != 1 {
		t.Log("string index of second target (3rd write, 1st & 2nd being dups) is not 1")
		t.Fail()
	}

	stringIdFour := stringStore.Intern(target)
	if stringIdFour != 0 {
		t.Log("string index of target being written 3rd time is not 0 l")
		t.Fail()
	}
}

func TestHashCollision(t *testing.T) {
	tmpDir := t.TempDir()
	stringStorePath := filepath.Join(tmpDir, "collision_store.bin")
	stringIndexStorePath := filepath.Join(tmpDir, "collision_index.bin")

	indexStore, err := CreateStringIndex(stringIndexStorePath, 1024)
	if err != nil {
		panic(err)
	}
	stringStore, err := CreateStringStore(stringStorePath, indexStore, 1024)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		_ = indexStore.Close()
		_ = stringStore.Close()
	})

	// --- MOCK HASH START ---
	originalHashString := hashString
	defer func() { hashString = originalHashString }()

	hashString = func(s string) uint64 {
		return 1337
	}
	// --- MOCK HASH END ---

	strA := "FirstString"
	strB := "SecondString" // different text, same hash (1337)

	idA := stringStore.Intern(strA)
	idB := stringStore.Intern(strB)

	if idA == idB {
		t.Fatal("Collision failure: Different strings with same hash were merged into same ID")
	}

	if len(stringStore.Hash[1337]) != 2 {
		t.Fatalf("Expected 2 entries in hash bucket, got %d", len(stringStore.Hash[1337]))
	}

	// 3. Suche String A nochmal (Muss im Bucket zwischen A und B den richtigen finden)
	idACheck := stringStore.Intern(strA)

	if idACheck != idA {
		t.Fatalf("Lookup failure in collision bucket: Expected ID %d, got %d", idA, idACheck)
	}
}

func TestResizing(t *testing.T) {
	tmpDir := t.TempDir()
	stringStorePath := filepath.Join(tmpDir, "resize_store.bin")
	stringIndexStorePath := filepath.Join(tmpDir, "resize_index.bin")

	indexStore, err := CreateStringIndex(stringIndexStorePath, 1024)
	if err != nil {
		panic(err)
	}
	stringStore, err := CreateStringStore(stringStorePath, indexStore, 24)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		_ = indexStore.Close()
		_ = stringStore.Close()
	})

	targetA := "sixteencharacter"

	Ida := stringStore.Intern(targetA)
	retrievedStringA := stringStore.Get(Ida)

	if targetA != retrievedStringA {
		t.Fatalf("String retrieval from store was not successful %v", retrievedStringA)
	}

	targetB := "test"

	Idb := stringStore.Intern(targetB)
	retrievedStringB := stringStore.Get(Idb)

	if targetB != retrievedStringB {
		t.Fatalf("String retrieval for 2nd string from store was not successful, resizing may failed %v", retrievedStringA)
	}

}
