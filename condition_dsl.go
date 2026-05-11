package q

func Eq(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "=",
		},
	}
}

func Ne(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "!=",
		},
	}
}

func Gt(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: ">",
		},
	}
}

func Ge(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: ">=",
		},
	}
}

func Lt(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "<",
		},
	}
}

func Le(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "<=",
		},
	}
}

func Like(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "like",
		},
	}
}

func NotLike(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "not like",
		},
	}
}

func In(vals ...any) *ListValueCondition {
	return &ListValueCondition{
		values: vals,
		operationalImpl: operationalImpl{
			op: "in",
		},
	}
}

func NotIn(vals ...any) *ListValueCondition {
	return &ListValueCondition{
		values: vals,
		operationalImpl: operationalImpl{
			op: "not in",
		},
	}
}

func IsNull() *NoValueCondition {
	return &NoValueCondition{
		operationalImpl: operationalImpl{
			op: "is null",
		},
	}
}

func IsNotNull() *NoValueCondition {
	return &NoValueCondition{
		operationalImpl: operationalImpl{
			op: "is not null",
		},
	}
}

func Between(val1, val2 any) *TwoValueCondition {
	return &TwoValueCondition{
		value1:    val1,
		value2:    val2,
		operator1: "between",
		operator2: "and",
	}
}

func NotBetween(val1, val2 any) *TwoValueCondition {
	return &TwoValueCondition{
		value1:    val1,
		value2:    val2,
		operator1: "not between",
		operator2: "and",
	}
}
