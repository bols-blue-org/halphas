package main

import (
	"encoding/json"
	"fmt"

	"github.com/HouzuoGuo/tiedot/db"
)

func createCollection(myDB *db.DB, collectionName string) {
	// Create two collections: User
	if err := myDB.Create(collectionName); err != nil {
		panic(err)
	}
	fmt.Printf("create %s collection\n", collectionName)
}

func initDB(myDB *db.DB) bool {
	var (
		noUserCollection              = true
		noCollectionDefaultCollection = true
	)
	// What collections do I now have?
	for _, name := range myDB.AllCols() {
		if name == "User" {
			noUserCollection = false
		} else if name == "CollectionDefault" {
			noCollectionDefaultCollection = false
		}
	}

	if noUserCollection {
		createCollection(myDB, "User")
	} else {
		fmt.Printf("Have a collection User\n")
	}

	if noCollectionDefaultCollection {
		createCollection(myDB, "CollectionDefault")
	} else {
		fmt.Printf("Have a collection CollectionDefault\n")
	}

	return noUserCollection || noCollectionDefaultCollection
}

func main() {
	// ****************** Collection Management ******************

	myDBDir := "./tmp/MyDatabase"

	// (Create if not exist) open a database
	myDB, err := db.OpenDB(myDBDir)
	if err != nil {
		panic(err)
	}

	initDB(myDB)

	// Start using a collection (the reference is valid until DB schema changes or Scrub is carried out)
	userCollection := myDB.Use("User")

	// Insert document (afterwards the docID uniquely identifies the document and will never change)
	docID, err := userCollection.Insert(map[string]interface{}{
		"name": "Go 1.2 is released",
		"url":  "golang.org"})
	if err != nil {
		panic(err)
	}

	// Read document
	readBack, err := userCollection.Read(docID)
	if err != nil {
		panic(err)
	}
	fmt.Println("Document", docID, "is", readBack)

	// Update document
	err = userCollection.Update(docID, map[string]interface{}{
		"name": "Go is very popular",
		"url":  "google.com"})
	if err != nil {
		panic(err)
	}

	// Process all documents (note that document order is undetermined)
	userCollection.ForEachDoc(func(id int, docContent []byte) (willMoveOn bool) {
		fmt.Println("Document", id, "is", string(docContent))
		return true  // move on to the next document OR
		return false // do not move on to the next document
	})

	// ****************** Index Management ******************
	// Indexes assist in many types of queries
	// Create index (path leads to document JSON attribute)
	if err := userCollection.Index([]string{"author", "name", "first_name"}); err != nil {
		panic(err)
	}
	if err := userCollection.Index([]string{"Title"}); err != nil {
		panic(err)
	}
	if err := userCollection.Index([]string{"Source"}); err != nil {
		panic(err)
	}
	if err := userCollection.Index([]string{"MetaData","Owner"}); err != nil {
		panic(err)
	}
	if err := userCollection.Index([]string{"MetaData","Group"}); err != nil {
		panic(err)
	}

	// What indexes do I have on collection A?
	for _, path := range userCollection.AllIndexes() {
		fmt.Printf("I have an index on path %v\n", path)
	}

	// Remove index
	if err := userCollection.Unindex([]string{"author", "name", "first_name"}); err != nil {
		panic(err)
	}

	// ****************** Queries ******************
	tmp := map[string]interface{}{"Owner": "user1", "Group": "test", "Acl": "0777" }
	// Prepare some documents for the query
	userCollection.Insert(map[string]interface{}{"Title": "New Go release", "Source": "golang.org", "Age": 3, "MetaData": tmp})
	userCollection.Insert(map[string]interface{}{"Title": "Kitkat is here", "Source": "google.com", "Age": 2, "MetaData": tmp})
	tmp = map[string]interface{}{"Owner": "user3", "Group": "test", "Acl": "0777" }
	userCollection.Insert(map[string]interface{}{"Title": "Good Slackware", "Source": "slackware.com", "Age": 1,"MetaData": tmp})

	var query interface{}
	json.Unmarshal([]byte(`[{"eq": "New Go release", "in": ["Title"]}, {"eq": "user3", "in": ["MetaData","Owner"]}]`), &query)

	queryResult := make(map[int]struct{}) // query result (document IDs) goes into map keys

	if err := db.EvalQuery(query, userCollection, &queryResult); err != nil {
		panic(err)
	}

	// Query result are document IDs
	for id := range queryResult {
		// To get query result document, simply read it
		readBack, err := userCollection.Read(id)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Query returned document %v\n", readBack)
	}

	// Gracefully close database
	if err := myDB.Close(); err != nil {
		panic(err)
	}
}
