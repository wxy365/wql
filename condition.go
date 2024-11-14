package q

type Condition interface {
	conditionIdentity()
}

type ColumnCompareCondition struct {
	rightColumn IColumn
	operationalImpl
}

func (c *ColumnCompareCondition) conditionIdentity() {}

type ListValueCondition struct {
	values []any
	operationalImpl
}

func (l *ListValueCondition) conditionIdentity() {}

type NoValueCondition struct {
	operationalImpl
}

func (n *NoValueCondition) conditionIdentity() {}

type SingleValueCondition struct {
	value any
	operationalImpl
}

func (s *SingleValueCondition) conditionIdentity() {}

type TwoValueCondition struct {
	value1    any
	value2    any
	operator1 string
	operator2 string
}

func (t *TwoValueCondition) conditionIdentity() {}

func (t *TwoValueCondition) Operator1() string {
	return t.operator1
}

func (t *TwoValueCondition) Operator2() string {
	return t.operator2
}

type SubSelectCondition struct {
	subSelect *Selection
	operationalImpl
}

func (s *SubSelectCondition) conditionIdentity() {}

type ExistsPredicate struct {
	selectionBuilder Builder[*Selection]
	operator         string
}
