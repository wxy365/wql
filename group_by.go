package q

import (
	"github.com/wxy365/basal/text"
)

type GroupByMdl struct {
	columns []IColumn
}

func (g *GroupByMdl) GetStatement(ctx *RenderCtx) *Statement {
	b := text.Build("group by ")
	switch len(g.columns) {
	case 0:
	case 1:
		b.Push(GetColFqn(ctx, g.columns[0]))
	default:
		b.Push(GetColFqn(ctx, g.columns[0]))
		for _, col := range g.columns[1:] {
			b.Push(", ", GetColFqn(ctx, col))
		}
	}
	return &Statement{
		stm: b.String(),
	}
}
