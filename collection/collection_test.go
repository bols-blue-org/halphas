package collection

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	err := CreateCollection("TestQueryCol", 0x10101)
	if err == nil {
		CreateUser("test_user", "test_group")
		testCol := UseCollection("TestQueryCol", "admin")
		testCol.Insert(map[string]interface{}{"Title": "New Go release", "Source": "golang.org", "Age": 3})
		testCol.Insert(map[string]interface{}{"Title": "Kitkat is here", "Source": "google.com", "Age": 2})
		testCol.Insert(map[string]interface{}{"Title": "Good Slackware", "Source": "slackware.com", "Age": 1})
		testCol = UseCollection("TestQueryCol", "test_user")
		testCol.Insert(map[string]interface{}{"Title": "New Go release", "Source": "golang.org", "Age": 3})
		testCol.Index([]string{"Title"})
	} else {
		log.Printf("pass create:%v", err)
	}
	os.Exit(m.Run())
}

func TestGetCollectionSetting(t *testing.T) {
	info, err := getCollectionSetting("TestQueryCol")
	if err == nil {
		fmt.Println(info)
	} else {
		t.Errorf("got error! %v", err)
	}
}

func TestGetUserInfo(t *testing.T) {
	info, err := getUserInfo("admin")
	if err == nil {
		fmt.Println(info)
	} else {
		t.Errorf("got error! %v", err)
	}
}

func TestSinpleQurey(t *testing.T) {
	user := halphasDB.Use(AppUserCollection)
	data, err := simpleQuery("test_user", "User", user)
	if err == nil {
		fmt.Print("TestUser result:")
		fmt.Println(data)
	} else {
		t.Errorf("got error! %v", err)
	}
}

func TestShowALLSetting(t *testing.T) {
	//CreateUser("test_user", "test_group")
	user := halphasDB.Use(SettingCollection)
	user.ForEachDoc(func(id int, docContent []byte) (willMoveOn bool) {
		fmt.Println("Setting Document", id, "is", string(docContent))
		return true
	})
}

func TestShowALLUser(t *testing.T) {
	//CreateUser("test_user", "test_group")
	user := halphasDB.Use(AppUserCollection)
	user.ForEachDoc(func(id int, docContent []byte) (willMoveOn bool) {
		fmt.Println("User Document", id, "is", string(docContent))
		return true
	})

}

func TestCanReadPermission(t *testing.T) {

	var targetAccessUser = TiedotCollection{user: "test_user", group: "test_group", permission: ReadUser | WriteUser}
	var metadata map[string]interface{}
	var permission = []int{ReadUser, ReadGroup, ReadAll}
	var user = []string{"admin", "test_user", "nobody"}
	var group = []string{"admin", "test_group", "nobody"}
	var result = [][]bool{{true, true, false},
		{true, true, false},
		{true, true, true}}
	metadata = map[string]interface{}{"User": "test_user", "Group": "test_group"}
	for i, v := range permission {
		for j, name := range user {
			targetAccessUser.user = name
			targetAccessUser.group = group[j]
			targetAccessUser.permission = v
			var expend = canReadPermission(targetAccessUser, metadata)
			if expend != result[i][j] {
				t.Errorf("miss actial=%v,\texpend=%v\t(permission=%s,name=%s)", result[i][j], expend, v, name)
			}
		}
	}
}

func TestCreateCollection(t *testing.T) {
	CreateCollection("Test", 0x10000)
	testCol := UseCollection("Test", "admin")
	testCol.Insert(map[string]interface{}{"Title": "New Go release", "Source": "golang.org", "Age": 3})
	testCol.Insert(map[string]interface{}{"Title": "Kitkat is here", "Source": "google.com", "Age": 2})
	testCol.Insert(map[string]interface{}{"Title": "Good Slackware", "Source": "slackware.com", "Age": 1})

	testCol.ForEachDoc(func(id int, docContent []byte) (willMoveOn bool) {
		fmt.Println("Document", id, "is", string(docContent))
		return true
	})
}

func TestQuery(t *testing.T) {
	testCol := UseCollection("TestQueryCol", "test_user")
	var query interface{}
	json.Unmarshal([]byte(`[{"eq": "New Go release", "in": ["Title"]}]`), &query)

	queryResult := make(map[int]struct{}) // query result (document IDs) goes into map keys

	if err := testCol.EvalQuery(query, &queryResult); err != nil {
		t.Errorf("%v", err)
	}

	// Query result are document IDs
	for id := range queryResult {
		// To get query result document, simply read it
		readBack, err := testCol.Read(id)
		if err != nil {
			t.Errorf("%v", err)
		}
		exp, err := json.Marshal(readBack)
		if err != nil {
			t.Errorf("%v", err)
		}
		if string(exp) != `{"Age":3,"MetaData":{"Group":"test_group","User":"test_user"},"Source":"golang.org","Title":"New Go release"}` {
			t.Errorf("Query returned document %v\n", string(exp))
		}
	}
}
