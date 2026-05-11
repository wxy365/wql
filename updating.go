package q

import (
	"github.com/wxy365/basal/ds/maps"
	"github.com/wxy365/basal/text"
)

type Updating struct {
	table   *Table
	where   *WhereMdl
	values  []*ValueColumnMapping
	limit   int
	orderBy *OrderByMdl
}

func (u *Updating) GetStatement(ctx *RenderCtx) *Statement {
	b := text.Build("update ", u.table.Name(), " set ")
	params := make(map[int]any)
	for i, val := range u.values {
		if i > 0 {
			b.Push(", ")
		}
		b.Push(val.column.Name(), " = ?")
		params[int(ctx.cnt.Incr())] = val.value
	}
	if u.where != nil {
		stm := u.where.GetStatement(ctx)
		b.Push(" ", stm.stm)
		params = maps.Merge(params, stm.params)
	}
	if u.orderBy != nil {
		stm := u.orderBy.GetStatement(ctx)
		b.Push(" ", stm.stm)
		params = maps.Merge(params, stm.params)
	}
	if u.limit > 0 {
		stm := (&PagingMdl{limit: u.limit}).GetStatement(ctx)
		b.Push(" ", stm.stm)
		params = maps.Merge(params, stm.params)
	}
	return &Statement{
		stm:    b.String(),
		params: params,
	}
}
