package q

// Avg creates an AVG(column) aggregate function from a column name string.
func Avg(name string) *AvgAg {
	return AvgC(Col(name))
}

// AvgC creates an AVG(column) aggregate function from an IColumn reference.
func AvgC(column IColumn) *AvgAg {
	res := &AvgAg{
		column: column,
	}
	res.column.namespace().IfPresent(func(ns string) {
		res.setNamespace(ns)
	})
	return res
}

// Max creates a MAX(column) aggregate function from a column name string.
func Max(name string) *MaxAg {
	return MaxC(Col(name))
}

// MaxC creates a MAX(column) aggregate function from an IColumn reference.
func MaxC(column IColumn) *MaxAg {
	res := &MaxAg{
		column: column,
	}
	res.column.namespace().IfPresent(func(ns string) {
		res.setNamespace(ns)
	})
	return res
}

// Min creates a MIN(column) aggregate function from a column name string.
func Min(name string) *MinAg {
	return MinC(Col(name))
}

// MinC creates a MIN(column) aggregate function from an IColumn reference.
func MinC(column IColumn) *MinAg {
	res := &MinAg{
		column: column,
	}
	res.column.namespace().IfPresent(func(ns string) {
		res.setNamespace(ns)
	})
	return res
}

// Sum creates a SUM(column) aggregate function from a column name string.
func Sum(name string) *SumAg {
	return SumC(Col(name))
}

// SumC creates a SUM(column) aggregate function from an IColumn reference.
func SumC(column IColumn) *SumAg {
	res := &SumAg{
		column: column,
	}
	res.column.namespace().IfPresent(func(ns string) {
		res.setNamespace(ns)
	})
	return res
}
