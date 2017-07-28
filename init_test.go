package main

import (
	"testing"
	"os"

	"github.com/HouzuoGuo/tiedot/db"
)

func TestInit(t *testing.T) {
	myDBDir := "./tmp/test/init"
	os.RemoveAll(myDBDir)
	defer os.RemoveAll(myDBDir)

	myDB, err := db.OpenDB(myDBDir)
	if err != nil {
		panic(err)
	}

	initDB(myDB)
}

func TestInitSkip(t *testing.T) {
        myDBDir := "./tmp/test/skip"
        myDB, err := db.OpenDB(myDBDir)
	if err != nil {
		panic(err)
	}


        initDB(myDB)
}

