package q

import (
	"github.com/wxy365/basal/opt"
)

// AvgAg represents a SQL AVG aggregate function: avg(column).
type AvgAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

// Name returns an empty string; AvgAg has no inherent column name.
func (a *AvgAg) Name() string {
	return ""
}

// Alias returns the optional alias of this aggregate expression.
func (a *AvgAg) Alias() opt.Opt[string] {
	return opt.Of(a.alias)
}

// As sets the alias for this aggregate expression and returns the IColumn for chaining.
func (a *AvgAg) As(alias string) IColumn {
	a.alias = alias
	return a
}

// GetExpr renders the AVG function call as a SQL string.
func (a *AvgAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, a.column, a.Alias(), "avg(", ")")
}

// MaxAg represents a SQL MAX aggregate function: max(column).
type MaxAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

// Name returns an empty string; MaxAg has no inherent column name.
func (m *MaxAg) Name() string {
	return ""
}

// Alias returns the optional alias of this aggregate expression.
func (m *MaxAg) Alias() opt.Opt[string] {
	return opt.Of(m.alias)
}

// As sets the alias for this aggregate expression and returns the IColumn for chaining.
func (m *MaxAg) As(alias string) IColumn {
	m.alias = alias
	return m
}

// GetExpr renders the MAX function call as a SQL string.
func (m *MaxAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, m.column, m.Alias(), "max(", ")")
}

// MinAg represents a SQL MIN aggregate function: min(column).
type MinAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

// Name returns an empty string; MinAg has no inherent column name.
func (m *MinAg) Name() string {
	return ""
}

// Alias returns the optional alias of this aggregate expression.
func (m *MinAg) Alias() opt.Opt[string] {
	return opt.Of(m.alias)
}

// As sets the alias for this aggregate expression and returns the IColumn for chaining.
func (m *MinAg) As(alias string) IColumn {
	m.alias = alias
	return m
}

// GetExpr renders the MIN function call as a SQL string.
func (m *MinAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, m.column, m.Alias(), "min(", ")")
}

// SumAg represents a SQL SUM aggregate function: sum(column).
type SumAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

// Name returns an empty string; SumAg has no inherent column name.
func (s *SumAg) Name() string {
	return ""
}

// Alias returns the optional alias of this aggregate expression.
func (s *SumAg) Alias() opt.Opt[string] {
	return opt.Of(s.alias)
}

// As sets the alias for this aggregate expression and returns the IColumn for chaining.
func (s *SumAg) As(alias string) IColumn {
	s.alias = alias
	return s
}

// GetExpr renders the SUM function call as a SQL string.
func (s *SumAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, s.column, s.Alias(), "sum(", ")")
}

// CountAg represents a SQL COUNT aggregate function: count(column).
type CountAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

// GetExpr renders the COUNT function call as a SQL string.
func (c *CountAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, c.column, c.Alias(), "count(", ")")
}

// Name returns an empty string; CountAg has no inherent column name.
func (c *CountAg) Name() string {
	return ""
}

// Alias returns the optional alias of this aggregate expression.
func (c *CountAg) Alias() opt.Opt[string] {
	return opt.Of(c.alias)
}

// As sets the alias for this aggregate expression and returns the IColumn for chaining.
func (c *CountAg) As(alias string) IColumn {
	c.alias = alias
	return c
}

// CountDistinctAg represents a SQL COUNT(DISTINCT column) aggregate function.
type CountDistinctAg struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

// Name returns an empty string; CountDistinctAg has no inherent column name.
func (c *CountDistinctAg) Name() string {
	return ""
}

// Alias returns the optional alias of this aggregate expression.
func (c *CountDistinctAg) Alias() opt.Opt[string] {
	return opt.Of(c.alias)
}

// As sets the alias for this aggregate expression and returns the IColumn for chaining.
func (c *CountDistinctAg) As(alias string) IColumn {
	c.alias = alias
	return c
}

// GetExpr renders the COUNT(DISTINCT column) function call as a SQL string.
func (c *CountDistinctAg) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, c.column, c.Alias(), "count(distinct ", ")")
}

// buildAliasableFunctionExpr builds a SQL function expression with optional alias.
func buildAliasableFunctionExpr(ctx *RenderCtx, column IColumn, alias opt.Opt[string], prefix, suffix string) string {
	stm := prefix + GetColFqnNamePreferred(ctx, column) + suffix
	return opt.Map(alias, func(alias string) string {
		return stm + " " + ctx.dbType.escaper()(alias)
	}).OrElse(stm)
}
