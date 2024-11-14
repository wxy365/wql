package q

import (
	"github.com/wxy365/basal/ds/maps"
	"github.com/wxy365/basal/text"
)

type Criterion interface {
	criterionIdentity()
	StatementProvider
}

type BaseCriterion struct {
	subCriteria []*AndOrCriteria
}

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

type AndOrCriteria struct {
	connector      string
	firstCriterion Criterion
	subCriteria    []*AndOrCriteria
}

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

type ColumnConditionCriterion struct {
	BaseCriterion
	column    IColumn
	condition Condition
}

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

type CriterionGroup struct {
	BaseCriterion
	firstCriterion Criterion
}

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

type ExistsCriterion struct {
	existsPredicate *ExistsPredicate
}

func (e *ExistsCriterion) criterionIdentity() {}

func (e *ExistsCriterion) GetStatement(ctx *RenderCtx) *Statement {
	selection := e.existsPredicate.selectionBuilder.Build()
	stm := selection.GetStatement(ctx)
	return &Statement{
		stm:    e.existsPredicate.operator + " (" + stm.stm + ")",
		params: stm.params,
	}
}

type NotCriterion struct {
	*BaseCriterion
}

func (n *NotCriterion) criterionIdentity() {}

func (n *NotCriterion) GetStatement(ctx *RenderCtx) *Statement {
	stm := n.BaseCriterion.GetStatement(ctx)
	return &Statement{
		stm:    "not (" + stm.stm + ")",
		params: stm.params,
	}
}
