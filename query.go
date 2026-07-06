package q

import (
	"github.com/wxy365/basal/ds/maps"
	"github.com/wxy365/basal/text"
)

// Selection represents the result of a complete SELECT query, which may
// consist of one or more QueryExprs combined via UNION / UNION ALL, along
// with optional ORDER BY and paging (LIMIT/OFFSET) clauses.
type Selection struct {
	queryExprs []*QueryExpr
	orderBy    *OrderByMdl
	paging     *PagingMdl
}

// GetStatement renders the Selection into a parameterized SQL Statement.
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

// QueryExpr represents a single SELECT query expression (a single SELECT
// statement without UNION, possibly with JOIN, WHERE, GROUP BY and HAVING).
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

// GetStatement renders the QueryExpr into a parameterized SQL Statement.
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

// UnionQuery represents a UNION or UNION ALL query that combines multiple
// SELECT statements into a single result set.
type UnionQuery struct {
	connector string
	selection *Selection
}
