package q

// Eq creates an equality condition: column = val.
func Eq(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "=",
		},
	}
}

// Ne creates an inequality condition: column != val.
func Ne(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "!=",
		},
	}
}

// Gt creates a greater-than condition: column > val.
func Gt(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: ">",
		},
	}
}

// Ge creates a greater-than-or-equal condition: column >= val.
func Ge(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: ">=",
		},
	}
}

// Lt creates a less-than condition: column < val.
func Lt(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "<",
		},
	}
}

// Le creates a less-than-or-equal condition: column <= val.
func Le(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "<=",
		},
	}
}

// Like creates a LIKE condition: column LIKE pattern.
func Like(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "like",
		},
	}
}

// NotLike creates a NOT LIKE condition: column NOT LIKE pattern.
func NotLike(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "not like",
		},
	}
}

// In creates an IN condition: column IN (val1, val2, ...).
func In(vals ...any) *ListValueCondition {
	return &ListValueCondition{
		values: vals,
		operationalImpl: operationalImpl{
			op: "in",
		},
	}
}

// NotIn creates a NOT IN condition: column NOT IN (val1, val2, ...).
func NotIn(vals ...any) *ListValueCondition {
	return &ListValueCondition{
		values: vals,
		operationalImpl: operationalImpl{
			op: "not in",
		},
	}
}

// ColumnCompare creates a column-to-column comparison condition: left_col = right_col.
func ColumnCompare(rightColumn IColumn, operator string) *ColumnCompareCondition {
	return &ColumnCompareCondition{
		rightColumn: rightColumn,
		operationalImpl: operationalImpl{
			op: operator,
		},
	}
}

// IsNull creates an IS NULL condition.
func IsNull() *NoValueCondition {
	return &NoValueCondition{
		operationalImpl: operationalImpl{
			op: "is null",
		},
	}
}

// IsNotNull creates an IS NOT NULL condition.
func IsNotNull() *NoValueCondition {
	return &NoValueCondition{
		operationalImpl: operationalImpl{
			op: "is not null",
		},
	}
}

// Between creates a BETWEEN condition: column BETWEEN val1 AND val2.
func Between(val1, val2 any) *TwoValueCondition {
	return &TwoValueCondition{
		value1:    val1,
		value2:    val2,
		operator1: "between",
		operator2: "and",
	}
}

// NotBetween creates a NOT BETWEEN condition: column NOT BETWEEN val1 AND val2.
func NotBetween(val1, val2 any) *TwoValueCondition {
	return &TwoValueCondition{
		value1:    val1,
		value2:    val2,
		operator1: "not between",
		operator2: "and",
	}
}

// EqSubQ creates an equality condition with a sub-query: column = (SELECT ...).
func EqSubQ(sb Builder[*Selection]) *SubSelectCondition {
	return &SubSelectCondition{
		subSelect: sb.Build(),
		operationalImpl: operationalImpl{
			op: "=",
		},
	}
}

// NeSubQ creates an inequality condition with a sub-query: column != (SELECT ...).
func NeSubQ(sb Builder[*Selection]) *SubSelectCondition {
	return &SubSelectCondition{
		subSelect: sb.Build(),
		operationalImpl: operationalImpl{
			op: "!=",
		},
	}
}

// GtSubQ creates a greater-than condition with a sub-query: column > (SELECT ...).
func GtSubQ(sb Builder[*Selection]) *SubSelectCondition {
	return &SubSelectCondition{
		subSelect: sb.Build(),
		operationalImpl: operationalImpl{
			op: ">",
		},
	}
}

// GeSubQ creates a greater-than-or-equal condition with a sub-query: column >= (SELECT ...).
func GeSubQ(sb Builder[*Selection]) *SubSelectCondition {
	return &SubSelectCondition{
		subSelect: sb.Build(),
		operationalImpl: operationalImpl{
			op: ">=",
		},
	}
}

// LtSubQ creates a less-than condition with a sub-query: column < (SELECT ...).
func LtSubQ(sb Builder[*Selection]) *SubSelectCondition {
	return &SubSelectCondition{
		subSelect: sb.Build(),
		operationalImpl: operationalImpl{
			op: "<",
		},
	}
}

// LeSubQ creates a less-than-or-equal condition with a sub-query: column <= (SELECT ...).
func LeSubQ(sb Builder[*Selection]) *SubSelectCondition {
	return &SubSelectCondition{
		subSelect: sb.Build(),
		operationalImpl: operationalImpl{
			op: "<=",
		},
	}
}

// InSubQ creates an IN condition with a sub-query: column IN (SELECT ...).
func InSubQ(sb Builder[*Selection]) *SubSelectCondition {
	return &SubSelectCondition{
		subSelect: sb.Build(),
		operationalImpl: operationalImpl{
			op: "in",
		},
	}
}

// NotInSubQ creates a NOT IN condition with a sub-query: column NOT IN (SELECT ...).
func NotInSubQ(sb Builder[*Selection]) *SubSelectCondition {
	return &SubSelectCondition{
		subSelect: sb.Build(),
		operationalImpl: operationalImpl{
			op: "not in",
		},
	}
}

// Exists creates an EXISTS predicate with a sub-query.
func Exists(sb Builder[*Selection]) *ExistsPredicate {
	return &ExistsPredicate{
		selectionBuilder: sb,
		operator:         "exists",
	}
}

// NotExists creates a NOT EXISTS predicate with a sub-query.
func NotExists(sb Builder[*Selection]) *ExistsPredicate {
	return &ExistsPredicate{
		selectionBuilder: sb,
		operator:         "not exists",
	}
}
