package q

import (
	"github.com/wxy365/basal/opt"
)

// ITable represents a table or table-like expression (e.g. a sub-query) in a
// SQL statement. Implementations must provide a name, alias support, and the
// ability to render themselves as a Statement.
type ITable interface {
	named
	aliasable[ITable]
	StatementProvider
	iTableIdentity()
}

type iTableAdapter struct {
}

func (i *iTableAdapter) iTableIdentity() {}

// Table represents a concrete database table with an optional alias.
// Create instances via Tbl().
type Table struct {
	iTableAdapter
	namedImpl
	alias string
}

// Alias returns the optional alias of this table.
func (t *Table) Alias() opt.Opt[string] {
	return opt.Of(t.alias)
}

// As sets the alias for this table and returns the ITable for chaining.
func (t *Table) As(alias string) ITable {
	t.alias = alias
	return t
}

// Col creates a Column scoped to this table, using the table's alias (or
// name) as the column namespace.
func (t *Table) Col(columnName string) *Column {
	col := &Column{}
	col.name = columnName
	col.setNamespace(t.Alias().OrElse(t.Name()))
	return col
}

// GetStatement renders the table reference as a SQL statement, including
// alias if set, with proper escaping.
func (t *Table) GetStatement(ctx *RenderCtx) *Statement {
	escaper := ctx.dbType.escaper()
	nameAlias := opt.Map(t.Alias(), func(alias string) string {
		return escaper(t.Name()) + " " + escaper(alias)
	}).OrElse(escaper(t.Name()))
	return &Statement{
		stm: nameAlias,
	}
}

// SubQuery represents a sub-query used as a table expression in a FROM or
// JOIN clause, with an assigned alias.
type SubQuery struct {
	iTableAdapter
	selection *Selection
	alias     string
}

// Name returns an empty string as sub-queries do not have a native name.
func (s *SubQuery) Name() string {
	return ""
}

// Alias returns the alias assigned to this sub-query.
func (s *SubQuery) Alias() opt.Opt[string] {
	return opt.Of(s.alias)
}

// As sets the alias for this sub-query and returns the ITable for chaining.
func (s *SubQuery) As(alias string) ITable {
	s.alias = alias
	return s
}

// GetStatement renders the sub-query as a parenthesized SELECT statement
// with its alias.
func (s *SubQuery) GetStatement(ctx *RenderCtx) *Statement {
	stm := s.selection.GetStatement(ctx)
	return &Statement{
		stm:    "(" + stm.stm + ") " + s.Alias().Get(),
		params: stm.params,
	}
}

// DO (Domain Object) is implemented by struct types that map to database
// tables. TableName() returns the corresponding table name.
type DO interface {
	TableName() string
}
