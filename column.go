package q

import (
	"github.com/wxy365/basal/opt"
)

// IColumn represents a column expression in a SQL statement. Implementations
// must provide a name, alias, namespace support, and the ability to render
// their expression.
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

// Column represents a database column reference with optional namespace
// (table name/alias) and alias. Create instances via Col().
type Column struct {
	iColumnAdapter
	namedImpl
	alias string
	namespaceAwareImpl
	desc bool
}

// Alias returns the optional alias of this column.
func (c *Column) Alias() opt.Opt[string] {
	return opt.Of(c.alias)
}

// As sets the alias for this column and returns the IColumn for chaining.
func (c *Column) As(alias string) IColumn {
	c.alias = alias
	return c
}

func (c *Column) isDesc() bool {
	return c.desc
}

// Desc returns this column as a descending-order sort specification.
func (c *Column) Desc() OrderBySpec {
	c.desc = true
	return c
}

// OrderByName renders the column's name for use in an ORDER BY clause,
// applying the provided escaper function and namespace prefix.
func (c *Column) OrderByName(escaper func(string) string) string {
	colName := escaper(c.Alias().OrElse(c.Name()))
	return opt.Map(
		c.namespace(),
		func(ns string) string {
			return escaper(ns) + "." + colName
		},
	).OrElse(colName)
}

// GetExpr renders the column as a SQL expression string, with optional
// namespace prefix and alias.
func (c *Column) GetExpr(ctx *RenderCtx) string {
	fqn := GetColFqnNamePreferred(ctx, c)
	return opt.Map(
		c.Alias(),
		func(alias string) string {
			return fqn + " " + ctx.dbType.escaper()(alias)
		},
	).OrElse(fqn)
}
