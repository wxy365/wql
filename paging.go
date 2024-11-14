package q

import (
	"strconv"
)

type PagingMdl struct {
	limit  int
	offset int
}

func (p *PagingMdl) GetStatement(ctx *RenderCtx) *Statement {
	var stm string
	switch ctx.dbType {
	case MySQL:
		if p.offset > 0 {
			if p.limit > 0 {
				stm = "limit " + strconv.Itoa(p.offset) + ", " + strconv.Itoa(p.limit)
			}
		} else {
			if p.limit > 0 {
				stm = "limit " + strconv.Itoa(p.limit)
			}
		}
	default:
		panic("unknown db type")
	}
	return &Statement{
		stm: stm,
	}
}
