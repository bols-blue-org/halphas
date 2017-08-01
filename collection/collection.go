package collection

import (
        "encoding/json"
	"fmt"

	"github.com/HouzuoGuo/tiedot/db"
)

func CreateCollection(collectionName string,permission int) error{
	if err := createCollection(halphasDB,collectionName); err != nil {
		return err
	}
	setting := halphasDB.Use("Setting")
	setting.Insert(map[string]interface{}{"Collection":collectionName,"Permission": permission})
	collection := halphasDB.Use(collectionName)
	list := map[string] interface{}{"IndexTMP": "IndexTMP"}
	list["MetaData"] = map[string]interface{}{"User": "admin", "Group": "admin"}
	collection.Insert(list)

	if err := collection.Index([]string{"MetaData","Owner"}); err != nil {
                return err
        }
        if err := collection.Index([]string{"MetaData","Group"}); err != nil {
                return err
        }
	return nil
}

func createCollection(myDB *db.DB, collectionName string) error{
	if err := myDB.Create(collectionName); err != nil {
		return err
	}
	fmt.Printf("create %s collection\n", collectionName)
	return nil
}

func UseCollection(name string, user string) Collection{
	tmp := halphasDB.Use(name)
	return &TiedotCollection {col: tmp, user: user, group: "admin", parmission: 0x10000}
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
		if name == "User" {
			noUserCollection = false
		} else if name == "Settings" {
			noSettingsCollection = false
		}
	}

	if noUserCollection {
		createCollection(myDB, "User")
		user := createUser("admin", "admin")
		user.Index([]string{"User"})
	} else {
		fmt.Printf("Have a collection User\n")
	}

	if noSettingsCollection {
		createCollection(myDB, "Settings")
	} else {
		fmt.Printf("Have a collection Settings\n")
	}

	return noUserCollection || noSettingsCollection
}

var halphasDB *db.DB

func init(){
	var err error;
	myDBDir := "./tmp/MyDatabase"

	// (Create if not exist) open a database
	halphasDB, err = db.OpenDB(myDBDir)
	if err != nil {
		panic(err)
	}
	initDB(halphasDB)
}

type TiedotCollection struct{
	col *db.Col
	user string
	group string
	parmission int
}

type Collection interface {
	Print()
	Index([]string) error
	Insert(map[string]interface {}) (int,error)
	ForEachDoc(func(int, []byte) bool)
	AllIndexes() [][]string
	Unindex([]string) error
	Read(int) (map[string]interface {},error)
	EvalQuery(query interface {},queryResult *map[int]struct {}) error
}

func (tie *TiedotCollection) Print() {
	fmt.Printf("%v",tie)
}

func (tie *TiedotCollection) Index(list []string) error{
	return tie.col.Index(list)
}

func (tie *TiedotCollection) Insert(list map[string]interface {}) (int,error){
	list["MetaData"] = map[string]interface{}{"User": tie.user, "Group": tie.group}
	return tie.col.Insert(list)
}

func (tie *TiedotCollection) ForEachDoc(fun func(id int, doc []byte) (moveOn bool)) {
	tie.col.ForEachDoc(fun)
}

func (tie *TiedotCollection) AllIndexes() [][]string {
	return tie.col.AllIndexes()
}

func (tie *TiedotCollection) Unindex(list []string) error{
	return tie.col.Unindex(list)
}

func (tie *TiedotCollection) Read(id int) (map[string]interface {},error){
	return tie.col.Read(id)
}

func (tie *TiedotCollection) EvalQuery(org_query interface {},queryResult *map[int]struct {}) error{
        var query interface{}
	q_str := fmt.Sprintf("{\"n\":[{\"eq\": \"%s\", \"in\": [\"MetaData\",\"Owner\"]}]}", tie.user)
	//fmt.Printf("q_str %v ", q_str)
        json.Unmarshal([]byte(q_str), &query)
	//fmt.Printf("query %v\n", query)
	q := query.(map[string] interface{})["n"].([] interface{})
	q = append(q, org_query)
	//query.(map[string] interface{})["n"] = q
	fmt.Printf("%v", query)
	return db.EvalQuery(org_query, tie.col, queryResult)
}
