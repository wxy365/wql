package q

func Avg(name string) *AvgAg {
	return AvgC(Col(name))
}

func AvgC(column IColumn) *AvgAg {
	res := &AvgAg{
		column: column,
	}
	res.column.namespace().IfPresent(func(ns string) {
		res.setNamespace(ns)
	})
	return res
}

func Max(name string) *MaxAg {
	return MaxC(Col(name))
}

func MaxC(column IColumn) *MaxAg {
	res := &MaxAg{
		column: column,
	}
	res.column.namespace().IfPresent(func(ns string) {
		res.setNamespace(ns)
	})
	return res
}

func Min(name string) *MinAg {
	return MinC(Col(name))
}

func MinC(column IColumn) *MinAg {
	res := &MinAg{
		column: column,
	}
	res.column.namespace().IfPresent(func(ns string) {
		res.setNamespace(ns)
	})
	return res
}

func Sum(name string) *SumAg {
	return SumC(Col(name))
}

func SumC(column IColumn) *SumAg {
	res := &SumAg{
		column: column,
	}
	res.column.namespace().IfPresent(func(ns string) {
		res.setNamespace(ns)
	})
	return res
}
