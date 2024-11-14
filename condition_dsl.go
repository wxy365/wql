package q

func Eq(val any) *SingleValueCondition {
	return &SingleValueCondition{
		value: val,
		operationalImpl: operationalImpl{
			op: "=",
		},
	}
}
