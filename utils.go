package q

import "github.com/wxy365/basal/opt"

// GetColFqn returns full qualified name of column, prefer using the column alias
func GetColFqn(ctx *RenderCtx, column IColumn) string {
	escaper := ctx.dbType.escaper()
	colName := escaper(column.Alias().OrElse(column.Name()))
	return opt.Map(
		column.namespace(),
		func(ns string) string {
			return escaper(ns) + "." + colName
		},
	).OrElse(colName)
}

// GetColFqnNamePreferred returns full qualified name of column, prefer using the column name
func GetColFqnNamePreferred(ctx *RenderCtx, column IColumn) string {
	escaper := ctx.dbType.escaper()
	colName := column.Name()
	if colName == "" {
		colName = column.Alias().Get()
	}
	if colName != "*" {
		colName = escaper(colName)
	}
	return opt.Map(
		column.namespace(),
		func(ns string) string {
			return escaper(ns) + "." + colName
		},
	).OrElse(colName)
}
