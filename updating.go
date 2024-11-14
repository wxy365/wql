package q

import (
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
	switch len(u.values) {
	case 1:
		b.Push(u.values[0].column.Name(), " = ?")
		params[int(ctx.cnt.Incr())] = u.values[0].value
	default:
		b.Push(u.values[0].column.Name(), " = ?")
		params[int(ctx.cnt.Incr())] = u.values[0].value
		for _, val := range u.values {
			b.Push(", ", val.column.Name(), " = ?")
			params[int(ctx.cnt.Incr())] = val.value
		}
	}
	return &Statement{
		stm:    b.String(),
		params: params,
	}
}
