package q

import (
	"testing"
)

type User struct {
	Id      int    `db:"id;auto_incr"`
	Name    string `db:"name"`
	Age     int    `db:"age"`
	Address string `db:"addr"`
}

func (u *User) TableName() string {
	return "user"
}

func TestInsert(t *testing.T) {
	u := &User{
		Id:      1,
		Name:    "wxy",
		Age:     18,
		Address: "xxx city",
	}
	insertion := InsertDO(u).Build()
	stm := insertion.GetStatement(getMysqlRenderCtx())
	if stm.stm != "insert into user(name, age, addr) values (?, ?, ?)" {
		t.Fail()
	}
	if stm.params[1] != "wxy" || stm.params[2] != 18 || stm.params[3] != "xxx city" {
		t.Fail()
	}
}
