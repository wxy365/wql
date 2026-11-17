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
		switch {
		case p.limit > 0 && p.offset > 0:
			stm = "limit " + strconv.Itoa(p.offset) + ", " + strconv.Itoa(p.limit)
		case p.limit > 0:
			stm = "limit " + strconv.Itoa(p.limit)
		case p.offset > 0:
			// MySQL requires a LIMIT to use an OFFSET, the largest possible
			// limit stands for "no limit at all".
			stm = "limit 18446744073709551615, " + strconv.Itoa(p.offset)
		}
	default:
		panic("unknown db type")
	}
	return &Statement{
		stm: stm,
	}
}
