package q

import (
	"github.com/wxy365/basal/opt"
)

type IColumn interface {
	aliasable[IColumn]
	named
	namespaceAware
	GetExpr(ctx *RenderCtx) string
	iColumnIdentity()
}

type iColumnAdapter struct {
}

func (i *iColumnAdapter) iColumnIdentity() {}

type Column struct {
	iColumnAdapter
	namedImpl
	alias string
	namespaceAwareImpl
	desc bool
}

func (c *Column) Alias() opt.Opt[string] {
	return opt.Of(c.alias)
}

func (c *Column) As(alias string) IColumn {
	c.alias = alias
	return c
}

func (c *Column) isDesc() bool {
	return c.desc
}

func (c *Column) Desc() OrderBySpec {
	c.desc = true
	return c
}

func (c *Column) OrderByName(escaper func(string) string) string {
	colName := escaper(c.Alias().OrElse(c.Name()))
	return opt.Map(
		c.namespace(),
		func(ns string) string {
			return escaper(ns) + "." + colName
		},
	).OrElse(colName)
}

func (c *Column) GetExpr(ctx *RenderCtx) string {
	fqn := GetColFqnNamePreferred(ctx, c)
	return opt.Map(
		c.Alias(),
		func(alias string) string {
			return fqn + " " + ctx.dbType.escaper()(alias)
		},
	).OrElse(fqn)
}
