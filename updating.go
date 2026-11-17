package q

import (
	"strconv"

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
	escaper := ctx.dbType.escaper()
	b := text.Build("update ", escaper(u.table.Name()), " set ")
	params := make(map[int]any)
	for i, val := range u.values {
		if i > 0 {
			b.Push(", ")
		}
		placeholder := ctx.cnt.Incr()
		b.Push(escaper(val.column.Name()), " = :", strconv.FormatInt(placeholder, 10))
		params[int(placeholder)] = val.value
	}
	if u.where != nil {
		stm := u.where.GetStatement(ctx)
		if stm.stm != "" {
			b.Push(" ", stm.stm)
		}
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
