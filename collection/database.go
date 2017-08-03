package collection

import (
	"log"
	"time"

	"github.com/HouzuoGuo/tiedot/db"
)

const (
	ReadFlag          = 0xff
	ReadUser          = 0x01
	ReadGroup         = 0x02
	ReadAll           = 0x10
	WriteFlag         = 0xff00
	WriteUser         = 0x0100
	WriteGroup        = 0x0200
	WriteDfedaltGroup = 0x0400
	WriteAll          = 0x1000
	SettingCollection = "CollectionSetting"
	AppUserCollection = "ApplicationUser"
)

func CreateCollection(collectionName string, permission int) error {
	if err := createCollection(halphasDB, collectionName); err != nil {
		return err
	}
	setting := halphasDB.Use(SettingCollection)
	if setting == nil {
		time.Sleep(3 * time.Second)
		setting = halphasDB.Use(SettingCollection)
		if setting == nil {
			log.Fatal("setting not exist.")
		}
	}
	setting.Insert(map[string]interface{}{"Collection": collectionName, "Permission": permission})
	collection := halphasDB.Use(collectionName)

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
	setting := halphasDB.Use(SettingCollection)
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

	return &TiedotCollection{col: tmp, user: info["User"].(string), group: info["Group"].(string), permission: pms}
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
		if name == AppUserCollection {
			noUserCollection = false
		} else if name == SettingCollection {
			noSettingsCollection = false
		}
	}

	if noUserCollection {
		createCollection(myDB, AppUserCollection)
		user := createUser("admin", "admin")
		user.Index([]string{"User"})
	} else {
		log.Printf("Have a collection %s\n", AppUserCollection)
	}

	if noSettingsCollection {
		createCollection(myDB, SettingCollection)
		CreateCollection("Sample", 0x0100)
		setting := halphasDB.Use(SettingCollection)
		setting.Index([]string{"Collection"})
	} else {
		log.Printf("Have a collection %s\n", SettingCollection)
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
