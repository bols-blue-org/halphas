package collection

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/HouzuoGuo/tiedot/db"
)

func simpleQuery(name string, fildName string, collection *db.Col) (map[string]interface{}, error) {
	var query interface{}
	// 使えない
	//query = map[string]interface{}{"eq": "admin", "in": []string{"User"}}
	//fmt.Println(query)
	qStr := fmt.Sprintf("{\"eq\": \"%s\", \"in\": [\"%s\"]}", name, fildName)
	json.Unmarshal([]byte(qStr), &query)
	queryResult := make(map[int]struct{})
	db.EvalQuery(query, collection, &queryResult)
	keys := reflect.ValueOf(queryResult).MapKeys()
	if len(keys) > 0 {
		id, _ := keys[0].Interface().(int)
		return collection.Read(id)
	}
	return nil, errors.New("not Entry")
}
