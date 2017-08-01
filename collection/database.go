package collection

import (
	"log"
	"time"

	"github.com/HouzuoGuo/tiedot/db"
)

const (
	READ_FLAG           = 0xff
	READ_USER           = 0x01
	READ_GROUP          = 0x02
	READ_ALL            = 0x10
	WRITE_FLAG          = 0xff00
	WRITE_USER          = 0x0100
	WRITE_GROUP         = 0x0200
	WRITE_DFEDALT_GROUP = 0x0400
	WRITE_ALL           = 0x1000
	SETTING_COLLECTION  = "CollectionSetting"
	USER_COLLECTION     = "ApplicationUser"
)

func CreateCollection(collectionName string, permission int) error {
	if err := createCollection(halphasDB, collectionName); err != nil {
		return err
	}
	setting := halphasDB.Use(SETTING_COLLECTION)
	if setting == nil {
		time.Sleep(3 * time.Second)
		setting = halphasDB.Use(SETTING_COLLECTION)
		if setting == nil {
			log.Fatal("setting not exist.")
		}
	}
	setting.Insert(map[string]interface{}{"Collection": collectionName, "Permission": permission})
	collection := halphasDB.Use(collectionName)
	list := map[string]interface{}{"IndexTMP": "IndexTMP"}
	list["MetaData"] = map[string]interface{}{"User": "admin", "Group": "admin"}
	collection.Insert(list)

	if err := collection.Index([]string{"MetaData", "User"}); err != nil {
		return err
	}
	if err := collection.Index([]string{"MetaData", "Group"}); err != nil {
		return err
	}
	return nil
}

func createCollection(myDB *db.DB, collectionName string) error {
	if err := myDB.Create(collectionName); err != nil {
		return err
	}
	log.Printf("create %s collection\n", collectionName)
	return nil
}

func getCollectionSetting(name string) (map[string]interface{}, error) {
	setting := halphasDB.Use(SETTING_COLLECTION)
	return simpleQuery(name, "Collection", setting)
}

func UseCollection(name string, user string) Collection {
	tmp := halphasDB.Use(name)
	setting, err := getCollectionSetting(name)
	pms := 0x10000
	if err != nil {
		log.Printf("%v", err)
	} else {
		log.Printf("%v", setting)
		pms, _ = setting["Permission"].(int)
	}
	info, err := getUserInfo(user)
	if err != nil {
		log.Printf("%v", err)
	}

	return &TiedotCollection{col: tmp, user: info["User"].(string), group: info["Group"].(string), parmission: pms}
}

func Close() error {
	return halphasDB.Close()
}

func initDB(myDB *db.DB) bool {
	var (
		noUserCollection     = true
		noSettingsCollection = true
	)
	for _, name := range myDB.AllCols() {
		if name == USER_COLLECTION {
			noUserCollection = false
		} else if name == SETTING_COLLECTION {
			noSettingsCollection = false
		}
	}

	if noUserCollection {
		createCollection(myDB, USER_COLLECTION)
		user := createUser("admin", "admin")
		user.Index([]string{"User"})
	} else {
		log.Printf("Have a collection %s\n", USER_COLLECTION)
	}

	if noSettingsCollection {
		createCollection(myDB, SETTING_COLLECTION)
		CreateCollection("Sample", 0x0100)
		setting := halphasDB.Use(SETTING_COLLECTION)
		setting.Index([]string{"Collection"})
	} else {
		log.Printf("Have a collection %s\n", SETTING_COLLECTION)
	}

	return noUserCollection || noSettingsCollection
}

var halphasDB *db.DB

func init() {
	var err error
	myDBDir := "./tmp/MyDatabase"

	// (Create if not exist) open a database
	halphasDB, err = db.OpenDB(myDBDir)
	if err != nil {
		panic(err)
	}
	initDB(halphasDB)
}
