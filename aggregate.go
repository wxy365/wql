package q

import (
	"github.com/wxy365/basal/opt"
)

type AvgAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

func (a *AvgAg) Name() string {
	return ""
}

func (a *AvgAg) Alias() opt.Opt[string] {
	return opt.Of(a.alias)
}

func (a *AvgAg) As(alias string) IColumn {
	a.alias = alias
	return a
}

func (a *AvgAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, a.column, a.Alias(), "avg(", ")")
}

type MaxAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

func (m *MaxAg) Name() string {
	return ""
}

func (m *MaxAg) Alias() opt.Opt[string] {
	return opt.Of(m.alias)
}

func (m *MaxAg) As(alias string) IColumn {
	m.alias = alias
	return m
}

func (m *MaxAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, m.column, m.Alias(), "max(", ")")
}

type MinAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

func (m *MinAg) Name() string {
	return ""
}

func (m *MinAg) Alias() opt.Opt[string] {
	return opt.Of(m.alias)
}

func (m *MinAg) As(alias string) IColumn {
	m.alias = alias
	return m
}

func (m *MinAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, m.column, m.Alias(), "min(", ")")
}

type SumAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

func (s *SumAg) Name() string {
	return ""
}

func (s *SumAg) Alias() opt.Opt[string] {
	return opt.Of(s.alias)
}

func (s *SumAg) As(alias string) IColumn {
	s.alias = alias
	return s
}

func (s *SumAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, s.column, s.Alias(), "sum(", ")")
}

type CountAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

func (c *CountAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, c.column, c.Alias(), "count(", ")")
}

func (c *CountAg) Name() string {
	return ""
}

func (c *CountAg) Alias() opt.Opt[string] {
	return opt.Of(c.alias)
}

func (c *CountAg) As(alias string) IColumn {
	c.alias = alias
	return c
}

type CountDistinctAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

func (c *CountDistinctAg) Name() string {
	return ""
}

func (c *CountDistinctAg) Alias() opt.Opt[string] {
	return opt.Of(c.alias)
}

func (c *CountDistinctAg) As(alias string) IColumn {
	c.alias = alias
	return c
}

func (c *CountDistinctAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, c.column, c.Alias(), "count(distinct ", ")")
}

func buildAliasableFunctionExpr(ctx *RenderCtx, column IColumn, alias opt.Opt[string], prefix, suffix string) string {
	stm := prefix + GetColFqnNamePreferred(ctx, column) + suffix
	return opt.Map(alias, func(alias string) string {
		return stm + " " + ctx.dbType.escaper()(alias)
	}).OrElse(stm)
}
