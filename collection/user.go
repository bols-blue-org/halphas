package collection

import (
	"errors"

	"github.com/HouzuoGuo/tiedot/db"
)

type User struct {
	user  string
	group string
}

func CreateUser(userName string, groupName string) error {
	if _, err := getUserInfo(userName); err == nil {
		return errors.New("user is exist.")
	}
	createUser(userName, groupName)
	return nil
}

func createUser(userName string, groupName string) *db.Col {
	user := halphasDB.Use(AppUserCollection)
	user.Insert(map[string]interface{}{"User": userName, "Group": groupName})
	return user
}

func getUserInfo(name string) (map[string]interface{}, error) {
	user := halphasDB.Use(AppUserCollection)
	return simpleQuery(name, "User", user)
}
