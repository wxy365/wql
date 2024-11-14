package q

import (
	"github.com/wxy365/basal/ds/maps"
	"github.com/wxy365/basal/text"
)

type JoinMdl struct {
	joinSpecs []*JoinSpec
}

func (j *JoinMdl) GetStatement(ctx *RenderCtx) *Statement {
	switch len(j.joinSpecs) {
	case 0:
	case 1:
		return j.joinSpecs[0].GetStatement(ctx)
	default:
		params := make(map[int]any, 0)
		stm := j.joinSpecs[0].GetStatement(ctx)
		b := text.Build(stm.stm)
		params = maps.Merge(params, stm.params)
		for _, js := range j.joinSpecs[1:] {
			jStm := js.GetStatement(ctx)
			b.Push(" ", jStm.stm)
			params = maps.Merge(params, jStm.params)
		}
		return &Statement{
			stm:    b.String(),
			params: params,
		}
	}
	return nil
}

type JoinSpec struct {
	table        ITable
	joinCriteria []*JoinCriterion
	joinType     JoinType
}

func (j *JoinSpec) GetStatement(ctx *RenderCtx) *Statement {
	params := make(map[int]any, 0)
	b := text.Build(j.joinType.String(), " ")
	tStm := j.table.GetStatement(ctx)
	b.Push(tStm.stm)
	params = maps.Merge(params, tStm.params)

	for _, c := range j.joinCriteria {
		cStm := c.GetStatement(ctx)
		b.Push(" ", cStm.stm)
		params = maps.Merge(params, cStm.params)
	}
	return &Statement{
		stm:    b.String(),
		params: params,
	}
}

type JoinCriterion struct {
	connector  string
	leftColumn IColumn
	condition  JoinCondition
}

func (j *JoinCriterion) GetStatement(ctx *RenderCtx) *Statement {
	params := make(map[int]any)
	b := text.Build(j.connector, " ", GetColFqn(ctx, j.leftColumn))
	if cond, ok := j.condition.(*JoinEqualTo); ok {
		b.Push(" ", cond.JoinOperator(), " ", GetColFqn(ctx, cond.rightColumn))
	} else if cond, ok := j.condition.(*JoinEqualToValue); ok {
		placeHolder := ctx.cnt.Incr()
		b.Push(" ", cond.JoinOperator(), " :").PushInt(int(placeHolder))
		params[int(placeHolder)] = cond.value
	}
	return &Statement{
		stm:    b.String(),
		params: params,
	}
}

type JoinCondition interface {
	JoinOperator() string
}

func EqualTo(column string) *JoinEqualTo {
	return &JoinEqualTo{
		rightColumn: Col(column),
	}
}

type JoinEqualTo struct {
	rightColumn IColumn
}

func (j *JoinEqualTo) JoinOperator() string {
	return "="
}

func EqualToValue(value any) *JoinEqualToValue {
	return &JoinEqualToValue{
		value: value,
	}
}

type JoinEqualToValue struct {
	value any
}

type Cond interface {
	*JoinEqualToValue | *JoinEqualTo | *ColumnCompareCondition | *ListValueCondition | *NoValueCondition | *SingleValueCondition
}

func AND(column string, condition JoinCondition) *JoinCriterion {
	return &JoinCriterion{
		connector:  "and",
		leftColumn: Col(column),
		condition:  condition,
	}
}

func (j *JoinEqualToValue) JoinOperator() string {
	return "="
}

type JoinType uint8

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
)

func (j JoinType) String() string {
	switch j {
	case InnerJoin:
		return "join"
	case LeftJoin:
		return "left join"
	case RightJoin:
		return "right join"
	case FullJoin:
		return "full join"
	default:
		panic("unknown join type")
	}
}
