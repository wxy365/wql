package q

import (
	"github.com/wxy365/basal/text"
)

type OrderByMdl struct {
	columns []OrderBySpec
}

func (o *OrderByMdl) GetStatement(ctx *RenderCtx) *Statement {
	b := text.Build("order by ")
	escaper := ctx.dbType.escaper()
	switch len(o.columns) {
	case 0:
	case 1:
		b.Push(o.columns[0].OrderByName(escaper))
		if o.columns[0].isDesc() {
			b.Push(" desc")
		}
	default:
		b.Push(o.columns[0].OrderByName(escaper))
		if o.columns[0].isDesc() {
			b.Push(" desc")
		}
		for _, c := range o.columns[1:] {
			b.Push(", ", c.OrderByName(escaper))
			if c.isDesc() {
				b.Push(" desc")
			}
		}
	}

	return &Statement{
		stm: b.String(),
	}
}

type OrderBySpec interface {
	orderAware
	Desc() OrderBySpec
	OrderByName(escaper func(string) string) string
}
