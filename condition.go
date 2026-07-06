package q

// Condition is the marker interface for all SQL WHERE conditions. Concrete
// implementations include SingleValueCondition, ListValueCondition,
// NoValueCondition, TwoValueCondition, ColumnCompareCondition and
// SubSelectCondition.
type Condition interface {
	conditionIdentity()
}

// ColumnCompareCondition is a condition that compares a column to another
// column using an operator (e.g. "=", "!=", "<").
type ColumnCompareCondition struct {
	rightColumn IColumn
	operationalImpl
}

func (c *ColumnCompareCondition) conditionIdentity() {}

// ListValueCondition is a condition that checks a column against a list of
// values (e.g. IN, NOT IN).
type ListValueCondition struct {
	values []any
	operationalImpl
}

func (l *ListValueCondition) conditionIdentity() {}

// NoValueCondition is a condition that does not require a value
// (e.g. IS NULL, IS NOT NULL).
type NoValueCondition struct {
	operationalImpl
}

func (n *NoValueCondition) conditionIdentity() {}

// SingleValueCondition is a condition that compares a column to a single
// value (e.g. =, !=, >, >=, <, <=, LIKE, NOT LIKE).
type SingleValueCondition struct {
	value any
	operationalImpl
}

func (s *SingleValueCondition) conditionIdentity() {}

// TwoValueCondition is a condition that requires two values and two
// operators (e.g. BETWEEN val1 AND val2, NOT BETWEEN val1 AND val2).
type TwoValueCondition struct {
	value1    any
	value2    any
	operator1 string
	operator2 string
}

func (t *TwoValueCondition) conditionIdentity() {}

// Operator1 returns the first operator string (e.g. "between").
func (t *TwoValueCondition) Operator1() string {
	return t.operator1
}

// Operator2 returns the second operator string (e.g. "and").
func (t *TwoValueCondition) Operator2() string {
	return t.operator2
}

// SubSelectCondition is a condition that uses a sub-query as the value
// (e.g. IN (SELECT ...), NOT IN (SELECT ...)).
type SubSelectCondition struct {
	subSelect *Selection
	operationalImpl
}

func (s *SubSelectCondition) conditionIdentity() {}

// ExistsPredicate represents an EXISTS or NOT EXISTS predicate with a
// sub-query.
type ExistsPredicate struct {
	selectionBuilder Builder[*Selection]
	operator         string
}
