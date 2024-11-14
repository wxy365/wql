package q

import (
	"fmt"
	"github.com/wxy365/basal/opt"
	"github.com/wxy365/basal/text"
)

// +-*/
type AddFn struct {
	iColumnAdapter
	columns []IColumn
	alias   string
	namespaceAwareImpl
}

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

func (d *DivideFn) GetExpr(ctx *RenderCtx) string {
	return buildMultiColExpr(ctx, d.columns, d.Alias(), " / ", "(", ")")
}

type MultiplyFn struct {
	iColumnAdapter
	columns []IColumn
	alias   string
	namespaceAwareImpl
}

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

type SubtractFn struct {
	iColumnAdapter
	columns []IColumn
	alias   string
	namespaceAwareImpl
}

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

type ConcatFn struct {
	iColumnAdapter
	columns []IColumn
	alias   string
	namespaceAwareImpl
}

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
		return stm + " " + alias
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
