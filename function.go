package q

import (
	"fmt"
	"github.com/wxy365/basal/opt"
	"github.com/wxy365/basal/text"
)

// +-*/
// AddFn represents a SQL addition expression: col1 + col2 + ...
type AddFn struct {
	iColumnAdapter
	columns []IColumn
	alias   string
	namespaceAwareImpl
}

// Name returns an empty string; AddFn has no inherent column name.
func (a *AddFn) Name() string {
	return ""
}

func (a *AddFn) Alias() opt.Opt[string] {
	return opt.Of(a.alias)
}

func (a *AddFn) As(alias string) IColumn {
	a.alias = alias
	return a
}

func (a *AddFn) GetExpr(ctx *RenderCtx) string {
	return buildMultiColExpr(ctx, a.columns, a.Alias(), " + ", "(", ")")
}

// DivideFn represents a SQL division expression: col1 / col2 / ...
type DivideFn struct {
	iColumnAdapter
	columns []IColumn
	alias   string
	namespaceAwareImpl
}

func (d *DivideFn) Name() string {
	return ""
}

func (d *DivideFn) Alias() opt.Opt[string] {
	return opt.Of(d.alias)
}

func (d *DivideFn) As(alias string) IColumn {
	d.alias = alias
	return d
}

// GetExpr renders the division expression as a SQL string.
func (d *DivideFn) GetExpr(ctx *RenderCtx) string {
	return buildMultiColExpr(ctx, d.columns, d.Alias(), " / ", "(", ")")
}

// MultiplyFn represents a SQL multiplication expression: col1 * col2 * ...
type MultiplyFn struct {
	iColumnAdapter
	columns []IColumn
	alias   string
	namespaceAwareImpl
}

// GetExpr renders the multiplication expression as a SQL string.
func (m *MultiplyFn) GetExpr(ctx *RenderCtx) string {
	return buildMultiColExpr(ctx, m.columns, m.Alias(), " * ", "(", ")")
}

func (m *MultiplyFn) Name() string {
	return ""
}

func (m *MultiplyFn) Alias() opt.Opt[string] {
	return opt.Of(m.alias)
}

func (m *MultiplyFn) As(alias string) IColumn {
	m.alias = alias
	return m
}

// SubtractFn represents a SQL subtraction expression: col1 - col2 - ...
type SubtractFn struct {
	iColumnAdapter
	columns []IColumn
	alias   string
	namespaceAwareImpl
}

// GetExpr renders the subtraction expression as a SQL string.
func (s *SubtractFn) GetExpr(ctx *RenderCtx) string {
	return buildMultiColExpr(ctx, s.columns, s.Alias(), " - ", "(", ")")
}

func (s *SubtractFn) Name() string {
	return ""
}

func (s *SubtractFn) Alias() opt.Opt[string] {
	return opt.Of(s.alias)
}

func (s *SubtractFn) As(alias string) IColumn {
	s.alias = alias
	return s
}

// ConcatFn represents a SQL CONCAT function: concat(col1, col2, ...).
type ConcatFn struct {
	iColumnAdapter
	columns []IColumn
	alias   string
	namespaceAwareImpl
}

// GetExpr renders the CONCAT function call as a SQL string.
func (c *ConcatFn) GetExpr(ctx *RenderCtx) string {
	return buildMultiColExpr(ctx, c.columns, c.Alias(), ", ", "concat(", ")")
}

func (c *ConcatFn) Name() string {
	return ""
}

func (c *ConcatFn) Alias() opt.Opt[string] {
	return opt.Of(c.alias)
}

func (c *ConcatFn) As(alias string) IColumn {
	c.alias = alias
	return c
}

// LowerFn represents a SQL LOWER function: lower(column).
type LowerFn struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

func (l *LowerFn) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, l.column, l.Alias(), "lower(", ")")
}

func (l *LowerFn) Name() string {
	return ""
}

func (l *LowerFn) Alias() opt.Opt[string] {
	return opt.Of(l.alias)
}

func (l *LowerFn) As(alias string) IColumn {
	l.alias = alias
	return l
}

// UpperFn represents a SQL UPPER function: upper(column).
type UpperFn struct {
	iColumnAdapter
	column IColumn
	alias  string
	namespaceAwareImpl
}

func (u *UpperFn) GetExpr(ctx *RenderCtx) string {
	return buildAliasableFunctionExpr(ctx, u.column, u.Alias(), "upper(", ")")
}

func (u *UpperFn) Name() string {
	return ""
}

func (u *UpperFn) Alias() opt.Opt[string] {
	return opt.Of(u.alias)
}

func (u *UpperFn) As(alias string) IColumn {
	u.alias = alias
	return u
}

// Substring represents a SQL SUBSTRING function: substring(column, offset, length).
type Substring struct {
	iColumnAdapter
	column IColumn
	alias  string
	offset int
	length int
	namespaceAwareImpl
}

func (s *Substring) GetExpr(ctx *RenderCtx) string {
	stm := fmt.Sprintf("substring(%s, %d, %d)", GetColFqn(ctx, s.column), s.offset, s.length)
	return opt.Map(s.Alias(), func(alias string) string {
		return stm + " " + ctx.dbType.escaper()(alias)
	}).OrElse(stm)
}

func (s *Substring) Name() string {
	return ""
}

func (s *Substring) Alias() opt.Opt[string] {
	return opt.Of(s.alias)
}

func (s *Substring) As(alias string) IColumn {
	s.alias = alias
	return s
}

func buildMultiColExpr(ctx *RenderCtx, columns []IColumn, alias opt.Opt[string], operator string, prefix, suffix string) string {
	stm := text.Join(columns, operator, func(column IColumn) string {
		return GetColFqnNamePreferred(ctx, column)
	}, prefix, suffix)
	return opt.Map(alias, func(alias string) string {
		return stm + " " + alias
	}).OrElse(stm)
}
