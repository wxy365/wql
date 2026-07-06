package q

import (
	"github.com/wxy365/basal/ds/maps"
	"github.com/wxy365/basal/text"
)

// Criterion is the interface for WHERE/HAVING clause criteria. Implementations
// must provide a StatementProvider that renders the criterion to SQL, and a
// private identity method for type safety.
type Criterion interface {
	criterionIdentity()
	StatementProvider
}

// BaseCriterion provides shared sub-criteria support for criterion types
// that need AND/OR chaining.
type BaseCriterion struct {
	subCriteria []*AndOrCriteria
}

// GetStatement renders all sub-criteria joined together.
func (b *BaseCriterion) GetStatement(ctx *RenderCtx) *Statement {
	bd := text.Build()
	params := make(map[int]any, 0)
	for _, sub := range b.subCriteria {
		stm := sub.GetStatement(ctx)
		bd.Push(" ", stm.stm)

		params = maps.Merge(params, stm.params)
	}
	return &Statement{
		stm:    bd.String(),
		params: params,
	}
}

func (b *BaseCriterion) criterionIdentity() {}

// AndOrCriteria represents a grouped set of criteria connected by AND or OR,
// rendered as "(criterion AND/OR criterion ...)".
type AndOrCriteria struct {
	connector      string
	firstCriterion Criterion
	subCriteria    []*AndOrCriteria
}

// GetStatement renders the grouped criteria wrapped in parentheses.
func (a *AndOrCriteria) GetStatement(ctx *RenderCtx) *Statement {
	b := text.Build()
	params := make(map[int]any)
	if a.connector != "" {
		b.Push(a.connector, " ")
	}
	b.Push("(")
	if a.firstCriterion != nil {
		stm := a.firstCriterion.GetStatement(ctx)
		b.Push(stm.stm)
		params = maps.Merge(params, stm.params)
	}
	if len(a.subCriteria) > 0 {
		for _, sub := range a.subCriteria {
			stm := sub.GetStatement(ctx)
			b.Push(" ", stm.stm)
			params = maps.Merge(params, stm.params)
		}
	}
	b.Push(")")
	return &Statement{
		stm:    b.String(),
		params: params,
	}
}

// ColumnConditionCriterion is a criterion that applies a Condition to a
// specific column.
type ColumnConditionCriterion struct {
	BaseCriterion
	column    IColumn
	condition Condition
}

// GetStatement renders the column condition criterion with parameterized
// values based on the Condition type.
func (c *ColumnConditionCriterion) GetStatement(ctx *RenderCtx) *Statement {
	fqn := GetColFqn(ctx, c.column)
	b := text.Build()
	params := make(map[int]any)
	if cond, ok := c.condition.(*SingleValueCondition); ok {
		placeHolder := ctx.cnt.Incr()
		b.Push(fqn, " ", cond.operator(), " :").PushInt(int(placeHolder))
		params[int(placeHolder)] = cond.value
	} else if cond, ok := c.condition.(*ColumnCompareCondition); ok {
		b.Push(fqn, " ", cond.operator(), " ", GetColFqn(ctx, cond.rightColumn))
	} else if cond, ok := c.condition.(*ListValueCondition); ok {
		b.Push(fqn, " ", cond.operator(), " (")
		switch len(cond.values) {
		case 0:
		case 1:
			placeHolder := ctx.cnt.Incr()
			b.Push(":").PushInt(int(placeHolder))
			params[int(placeHolder)] = cond.values[0]
		default:
			placeHolder := ctx.cnt.Incr()
			b.Push(":").PushInt(int(placeHolder))
			params[int(placeHolder)] = cond.values[0]

			for _, val := range cond.values[1:] {
				placeHolder = ctx.cnt.Incr()
				b.Push(", :").PushInt(int(placeHolder))
				params[int(placeHolder)] = val
			}
		}
		b.Push(")")
	} else if cond, ok := c.condition.(*NoValueCondition); ok {
		b.Push(fqn, " ", cond.operator())
	} else if cond, ok := c.condition.(*TwoValueCondition); ok {
		placeHolder1 := ctx.cnt.Incr()
		placeHolder2 := ctx.cnt.Incr()
		b.Push(fqn, " ", cond.Operator1(), " :").PushInt(int(placeHolder1)).
			Push(" ", cond.Operator2(), " :").PushInt(int(placeHolder2))
		params[int(placeHolder1)] = cond.value1
		params[int(placeHolder2)] = cond.value2
	} else if cond, ok := c.condition.(*SubSelectCondition); ok {
		subStm := cond.subSelect.GetStatement(ctx)
		b.Push(fqn, " ", cond.operator(), " (", subStm.stm, ")")
		params = maps.Merge(params, subStm.params)
	}

	return &Statement{
		stm:    b.String(),
		params: params,
	}
}

func (c *ColumnConditionCriterion) criterionIdentity() {}

// CriterionGroup groups a first criterion with additional sub-criteria,
// rendered as a flat sequence.
type CriterionGroup struct {
	BaseCriterion
	firstCriterion Criterion
}

// GetStatement renders the criterion group as a flat sequence of
// first criterion followed by sub-criteria.
func (c *CriterionGroup) GetStatement(ctx *RenderCtx) *Statement {
	b := text.Build()
	params := make(map[int]any)
	if c.firstCriterion != nil {
		stm := c.firstCriterion.GetStatement(ctx)
		b.Push(stm.stm)
		params = stm.params
	}
	if len(c.subCriteria) > 0 {
		for _, cri := range c.subCriteria {
			stm := cri.GetStatement(ctx)
			b.Push(" ", stm.stm)
			params = maps.Merge(params, stm.params)
		}
	}
	return &Statement{
		stm:    b.String(),
		params: params,
	}
}

func (c *CriterionGroup) criterionIdentity() {}

// ExistsCriterion represents an EXISTS or NOT EXISTS criterion with a
// sub-query.
type ExistsCriterion struct {
	existsPredicate *ExistsPredicate
}

func (e *ExistsCriterion) criterionIdentity() {}

// GetStatement renders the EXISTS/NOT EXISTS sub-query criterion.
func (e *ExistsCriterion) GetStatement(ctx *RenderCtx) *Statement {
	selection := e.existsPredicate.selectionBuilder.Build()
	stm := selection.GetStatement(ctx)
	return &Statement{
		stm:    e.existsPredicate.operator + " (" + stm.stm + ")",
		params: stm.params,
	}
}

// NotCriterion wraps another criterion with a NOT operator.
type NotCriterion struct {
	*BaseCriterion
}

func (n *NotCriterion) criterionIdentity() {}

// GetStatement wraps the inner criterion in "NOT (...)".
func (n *NotCriterion) GetStatement(ctx *RenderCtx) *Statement {
	stm := n.BaseCriterion.GetStatement(ctx)
	return &Statement{
		stm:    "not (" + stm.stm + ")",
		params: stm.params,
	}
}
