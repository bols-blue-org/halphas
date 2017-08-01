package collection

import (
        "encoding/json"
	"testing"
	"os"
	"fmt"

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

func TestCreateCollection(t *testing.T) {
	CreateCollection("Test",0x10000)
        testCol := UseCollection("Test", "admin")
	fmt.Printf("%v",testCol)
	testCol.Insert(map[string]interface{}{"Title": "New Go release", "Source": "golang.org", "Age": 3})
	testCol.ForEachDoc(func(id int, docContent []byte) (willMoveOn bool) {
                fmt.Println("Document", id, "is", string(docContent))
                return true
        })
}

func TestQuery(t *testing.T) {
	CreateCollection("Test",0x10000)
        testCol := UseCollection("Test", "test_user")
	testCol.Index([]string{"Title"})
	testCol.Index([]string{"MetaData","Owner"})
	testCol.Insert(map[string]interface{}{"Title": "New Go release", "Source": "golang.org", "Age": 3})
	var query interface{}
        json.Unmarshal([]byte(`[{"eq": "New Go release", "in": ["Title"]}]`), &query)

        queryResult := make(map[int]struct{}) // query result (document IDs) goes into map keys

        if err := testCol.EvalQuery(query, &queryResult); err != nil {
                panic(err)
        }

        // Query result are document IDs
        for id := range queryResult {
                // To get query result document, simply read it
                readBack, err := testCol.Read(id)
                if err != nil {
                        panic(err)
                }
                fmt.Printf("Query returned document %v\n", readBack)
        }

}

