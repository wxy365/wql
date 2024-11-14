package q

import (
	"github.com/wxy365/basal/fn"
	"testing"
)

func TestBuildCount(t *testing.T) {
	b := CountC(Col("name").As("name1")).As("cnt").
		FromT(Tbl("user").As("user1")).Where(Col("id"), Eq("wxy"))
	s := b.Build()
	stm := s.GetStatement(&RenderCtx{
		dbType: MySQL,
		cnt:    new(fn.Counter),
	})
	if stm.stm != "select count(`user1`.`name`) `cnt` from `user` `user1` where `id` = :1" {
		t.Fail()
	}
	if len(stm.params) == 0 || stm.params[1] != "wxy" {
		t.Fail()
	}
}
