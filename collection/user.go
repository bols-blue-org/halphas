package collection

import(
	"github.com/HouzuoGuo/tiedot/db"
)

type User struct {
	user string
	group string
}

func createUser(userName string, groupName string) *db.Col {
	user := halphasDB.Use("User")
	user.Insert(map[string]interface{}{"User": userName, "Group": groupName})
	return user
}

