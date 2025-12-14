package storage

/*func TestWriteOffset(t *testing.T) {

	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test_string_store.bin")

	stringStore, err := CreateStringStore(testFilePath)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		err := stringStore.Close()
		if err != nil {
			t.Logf("Error closing store: %v", err)
		}
	})

	writeOffset := readHeaderOffset(stringStore.Data)
	fmt.Println(writeOffset)

	err = stringStore.Close()
	if err != nil {
		panic(err)
	}
}*/
