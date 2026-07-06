package q

import (
	"github.com/wxy365/basal/opt"
)

// SubQueryColumn represents a scalar subquery used as a column expression
// in a SELECT list, rendered as "(SELECT ...) AS alias".
type SubQueryColumn struct {
	iColumnAdapter
	selection *Selection
	alias     string
}

// Name returns an empty string; SubQueryColumn has no inherent column name.
func (s *SubQueryColumn) Name() string {
	return ""
}

// Alias returns the optional alias of this subquery column.
func (s *SubQueryColumn) Alias() opt.Opt[string] {
	return opt.Of(s.alias)
}

// As sets the alias for this subquery column and returns the IColumn for chaining.
func (s *SubQueryColumn) As(alias string) IColumn {
	s.alias = alias
	return s
}

func (s *SubQueryColumn) namespace() opt.Opt[string] {
	return opt.Empty[string]()
}

func (s *SubQueryColumn) setNamespace(ns string) {
}

// GetExpr renders the scalar subquery as a SQL expression: "(SELECT ...) alias".
func (s *SubQueryColumn) GetExpr(ctx *RenderCtx) string {
	stm := s.selection.GetStatement(ctx)
	expr := "(" + stm.stm + ")"
	return opt.Map(s.Alias(), func(alias string) string {
		return expr + " " + ctx.dbType.escaper()(alias)
	}).OrElse(expr)
}

// SubQ creates a SubQueryColumn from a Selection builder.
// The resulting column can be used as a column expression in SELECT lists
// or as a column reference in WHERE/HAVING clauses.
func SubQ(sb Builder[*Selection]) *SubQueryColumn {
	return &SubQueryColumn{
		selection: sb.Build(),
	}
}
