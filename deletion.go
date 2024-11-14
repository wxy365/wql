package q

import (
	"github.com/wxy365/basal/ds/maps"
	"github.com/wxy365/basal/text"
)

type Deletion struct {
	table   *Table
	where   *WhereMdl
	orderBy *OrderByMdl
	limit   int
}

func (d *Deletion) GetStatement(ctx *RenderCtx) *Statement {
	escaper := ctx.dbType.escaper()
	params := make(map[int]any, 0)
	b := text.Build("delete from ", escaper(d.table.name))
	if alias := d.table.Alias(); alias.IsPresent() {
		b.Push(" ", alias.Get())
	}
	if d.where != nil {
		whereStm := d.where.GetStatement(ctx)
		b.Push(" ", whereStm.stm)
		params = maps.Merge(params, whereStm.params)
	}
	return &Statement{
		stm:    b.String(),
		params: params,
	}
}
