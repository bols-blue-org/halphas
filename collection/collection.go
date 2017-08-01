package collection

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/HouzuoGuo/tiedot/db"
)

type TiedotCollection struct {
	col        *db.Col
	user       string
	group      string
	parmission int
}

type Collection interface {
	Print()
	Index([]string) error
	Insert(map[string]interface{}) (int, error)
	ForEachDoc(func(int, []byte) bool)
	AllIndexes() [][]string
	Unindex([]string) error
	Read(int) (map[string]interface{}, error)
	EvalQuery(query interface{}, queryResult *map[int]struct{}) error
}

func (tie *TiedotCollection) Print() {
	log.Printf("%v", tie)
}

func (tie *TiedotCollection) Index(list []string) error {
	return tie.col.Index(list)
}

func (tie *TiedotCollection) Insert(list map[string]interface{}) (int, error) {
	list["MetaData"] = map[string]interface{}{"User": tie.user, "Group": tie.group}
	return tie.col.Insert(list)
}

func (tie *TiedotCollection) ForEachDoc(fun func(id int, doc []byte) (moveOn bool)) {
	tie.col.ForEachDoc(fun)
}

func (tie *TiedotCollection) AllIndexes() [][]string {
	return tie.col.AllIndexes()
}

func (tie *TiedotCollection) Unindex(list []string) error {
	return tie.col.Unindex(list)
}

func (tie *TiedotCollection) Read(id int) (map[string]interface{}, error) {
	return tie.col.Read(id)
}

func (tie *TiedotCollection) EvalQuery(orgQuery interface{}, queryResult *map[int]struct{}) error {
	var query interface{}
	qStr := fmt.Sprintf("[{\"eq\": \"%s\", \"in\": [\"MetaData\",\"User\"]}]", tie.user)
	json.Unmarshal([]byte(qStr), &query)
	q := query.([]interface{})
	q = append(q, orgQuery)
	queryBase := map[string]interface{}{"n": q}
	return db.EvalQuery(queryBase, tie.col, queryResult)
}
