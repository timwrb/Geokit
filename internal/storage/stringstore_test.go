package storage

import (
	"testing"
)

func TestStringStore(t *testing.T) {
	stringStore := storage.CreateStringStore()

	targetOne := "addr:housenumber"
	targetTwo := "addr:street"

	targetOneId := PutString(&targetOne, stringStore.Data)

}
