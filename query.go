package q

import (
	"github.com/wxy365/basal/ds/maps"
	"github.com/wxy365/basal/text"
)

type Selection struct {
	queryExprs []*QueryExpr
	orderBy    *OrderByMdl
	paging     *PagingMdl
}

func (s *Selection) GetStatement(ctx *RenderCtx) *Statement {
	b := text.Build()
	params := make(map[int]any)
	switch len(s.queryExprs) {
	case 0:
		return nil
	case 1:
		stm := s.queryExprs[0].GetStatement(ctx)
		b.Push(stm.stm)
		params = stm.params
	default:
		stm := s.queryExprs[0].GetStatement(ctx)
		b.Push(stm.stm)
		params = stm.params
		for _, qe := range s.queryExprs[1:] {
			stm = qe.GetStatement(ctx)
			b.Push(" ", stm.stm)
			params = maps.Merge(params, stm.params)
		}
	}

	if s.orderBy != nil {
		stm := s.orderBy.GetStatement(ctx)
		b.Push(" ", stm.stm)
	}

	if s.paging != nil {
		stm := s.paging.GetStatement(ctx)
		b.Push(" ", stm.stm)
	}
	return &Statement{
		stm:    b.String(),
		params: params,
	}
}

type QueryExpr struct {
	connector  string
	distinct   bool
	selectList []IColumn
	table      ITable
	join       *JoinMdl
	where      *WhereMdl
	groupBy    *GroupByMdl
	having     *HavingMdl
}

func (q *QueryExpr) GetStatement(ctx *RenderCtx) *Statement {
	b := text.Build()
	params := make(map[int]any)
	var suffix string
	if q.connector != "" {
		b.Push(q.connector, " (")
		suffix = ")"
	}

	b.Push("select")
	if q.distinct {
		b.Push(" distinct")
	}
	switch len(q.selectList) {
	case 0:
		b.Push(" *")
	case 1:
		b.Push(" ", q.selectList[0].GetExpr(ctx))
	default:
		b.Push(" ", q.selectList[0].GetExpr(ctx))
		for _, col := range q.selectList[1:] {
			b.Push(", ", col.GetExpr(ctx))
		}
	}
	b.Push(" from ")

	stm := q.table.GetStatement(ctx)
	b.Push(stm.stm)
	params = maps.Merge(params, stm.params)

	if q.join != nil {
		stm = q.join.GetStatement(ctx)
		b.Push(" " + stm.stm)
		params = maps.Merge(params, stm.params)
	}

	if q.where != nil {
		stm = q.where.GetStatement(ctx)
		b.Push(" " + stm.stm)
		params = maps.Merge(params, stm.params)
	}

	if q.groupBy != nil {
		stm = q.groupBy.GetStatement(ctx)
		b.Push(" " + stm.stm)
	}

	if q.having != nil {
		stm = q.having.GetStatement(ctx)
		b.Push(" " + stm.stm)
		params = maps.Merge(params, stm.params)
	}

	b.Push(suffix)

	return &Statement{
		stm:    b.String(),
		params: params,
	}
}

type UnionQuery struct {
	connector string
	selection *Selection
}
