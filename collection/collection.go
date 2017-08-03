package collection

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/HouzuoGuo/tiedot/db"
)

type TiedotCollection struct {
	col        *db.Col
	user       string
	group      string
	permission int
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

func canReadPermission(tc TiedotCollection, metaData map[string]interface{}) bool {
	if tc.group == "admin" {
		return true
	}

	switch tc.permission & ReadFlag {
	case ReadUser:
		userStr, ok := metaData["User"].(string)
		if !ok {
			log.Println("type assert miss")
			return false
		}
		return tc.user == userStr
	case ReadGroup:
		groupStr, ok := metaData["Group"].(string)
		if !ok {
			log.Println("type assert miss")
			return false
		}
		return tc.group == groupStr
	case ReadAll:
		return true
	default:
		return false
	}
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
	data, err := tie.col.Read(id)
	if err != nil {
		return data, err
	}
	assert, ok := data["MetaData"].(map[string]interface{})
	if !ok {
		return nil, errors.New("can't type assertion")
	}

	if !canReadPermission(*tie, assert) {
		return nil, errors.New("can't access permission.")
	}
	return data, err
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
