package q

import (
	"github.com/wxy365/basal/opt"
)

type ITable interface {
	named
	aliasable[ITable]
	StatementProvider
	iTableIdentity()
}

type iTableAdapter struct {
}

func (i *iTableAdapter) iTableIdentity() {}

type Table struct {
	iTableAdapter
	namedImpl
	alias string
}

func (t *Table) Alias() opt.Opt[string] {
	return opt.Of(t.alias)
}

func (t *Table) As(alias string) ITable {
	t.alias = alias
	return t
}

func (t *Table) Col(columnName string) *Column {
	col := &Column{}
	col.name = columnName
	return col
}

func (t *Table) GetStatement(ctx *RenderCtx) *Statement {
	escaper := ctx.dbType.escaper()
	nameAlias := opt.Map(t.Alias(), func(alias string) string {
		return escaper(t.Name()) + " " + escaper(alias)
	}).OrElse(escaper(t.Name()))
	return &Statement{
		stm: nameAlias,
	}
}

type SubQuery struct {
	iTableAdapter
	selection *Selection
	alias     string
}

func (s *SubQuery) Name() string {
	return ""
}

func (s *SubQuery) Alias() opt.Opt[string] {
	return opt.Of(s.alias)
}

func (s *SubQuery) As(alias string) ITable {
	s.alias = alias
	return s
}

func (s *SubQuery) GetStatement(ctx *RenderCtx) *Statement {
	stm := s.selection.GetStatement(ctx)
	return &Statement{
		stm:    "(" + stm.stm + ") " + s.Alias().Get(),
		params: stm.params,
	}
}

type DO interface {
	TableName() string
}
