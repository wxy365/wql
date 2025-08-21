package q

import (
	"context"

	"github.com/wxy365/basal/errs"
	"github.com/wxy365/basal/fn"
)

type CountFromGather struct {
	column   IColumn
	distinct bool
	alias    string
}

type CountDsl struct {
	column       IColumn
	distinct     bool
	alias        string
	table        ITable
	whereBuilder *CountWhereBuilder
	joinSpecs    []*JoinSpec
}

func (c *CountDsl) Action(ctx context.Context, optDb ...*DB) (uint64, error) {
	db := DataSource
	if len(optDb) > 0 {
		db = optDb[0]
	}
	var ret uint64
	renderCtx := &RenderCtx{
		dbType: db.dbType,
		cnt:    new(fn.Counter),
	}
	stm, args := c.Build().GetStatement(renderCtx).prepare()
	row := db.QueryRowContext(ctx, stm, args...)
	err := row.Scan(&ret)
	return ret, errs.Wrap(err, "Error in counting. sql: {0}, parameters: {1}], stm, args)
}

func (c *CountDsl) ActionTx(ctx context.Context, tx *TX) (uint64, error) {
	var ret uint64
	renderCtx := &RenderCtx{
		dbType: tx.dbType,
		cnt:    new(fn.Counter),
	}
	stm, args := c.Build().GetStatement(renderCtx).prepare()
	row := tx.QueryRowContext(ctx, stm, args...)
	err := row.Scan(&ret)
	return ret, errs.Wrap(err, "Error in counting. sql: {0}, parameters: {1}], stm, args)
}

type CountWhereBuilder struct {
	firstCriterion Criterion
	subCriteria    []*AndOrCriteria
	countDsl       *CountDsl
}

func (c *CountWhereBuilder) Action(ctx context.Context, optDb ...*DB) (uint64, error) {
	return c.countDsl.Action(ctx, optDb...)
}

func (c *CountWhereBuilder) ActionTx(ctx context.Context, tx *TX) (uint64, error) {
	return c.countDsl.ActionTx(ctx, tx)
}

func Count(column string) *CountFromGather {
	return CountC(Col(column))
}
func CountC(column IColumn) *CountFromGather {
	return &CountFromGather{
		column:   column,
		distinct: false,
	}
}

func CountDistinct(column string) *CountFromGather {
	return CountDistinctC(Col(column))
}
func CountDistinctC(column IColumn) *CountFromGather {
	return &CountFromGather{
		column:   column,
		distinct: true,
	}
}

func CountFrom(table string) *CountDsl {
	return CountFromT(Tbl(table))
}
func CountFromT(table ITable) *CountDsl {
	col := Col("*")
	col.setNamespace(table.Alias().OrElse(table.Name()))
	return &CountDsl{
		column:   col,
		distinct: false,
		table:    table,
	}
}

func CountFromDO(do DO) *CountDsl {
	return CountFrom(do.TableName())
}

func (c *CountFromGather) As(alias string) *CountFromGather {
	c.alias = alias
	return c
}

func (c *CountFromGather) From(table string) *CountDsl {
	return c.FromT(Tbl(table))
}

func (c *CountFromGather) FromT(table ITable) *CountDsl {
	c.column.setNamespace(table.Alias().OrElse(table.Name()))
	return &CountDsl{
		column:   c.column,
		distinct: c.distinct,
		alias:    c.alias,
		table:    table,
	}
}

func (c *CountFromGather) FromDO(do DO) *CountDsl {
	return c.From(do.TableName())
}

func (c *CountDsl) As(alias string) *CountDsl {
	c.alias = alias
	return c
}

func (c *CountDsl) Wheres() *CountWhereBuilder {
	b := &CountWhereBuilder{
		countDsl: c,
	}
	c.whereBuilder = b
	return b
}

func (c *CountDsl) Where(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *CountWhereBuilder {
	b := &CountWhereBuilder{
		countDsl: c,
		firstCriterion: &ColumnConditionCriterion{
			column:    column,
			condition: condition,
		},
		subCriteria: subCriteria,
	}
	c.whereBuilder = b
	return b
}

func (c *CountDsl) WhereE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *CountWhereBuilder {
	b := &CountWhereBuilder{
		countDsl: c,
		firstCriterion: &ExistsCriterion{
			existsPredicate: predicate,
		},
		subCriteria: subCriteria,
	}
	c.whereBuilder = b
	return b
}

func (c *CountDsl) WhereC(criterion Criterion, subCriteria ...*AndOrCriteria) *CountWhereBuilder {
	b := &CountWhereBuilder{
		countDsl:       c,
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	}
	c.whereBuilder = b
	return b
}

func (c *CountDsl) Join(table *Table, onJoinCriterion *JoinCriterion, andJoinCriterion ...*JoinCriterion) *CountDsl {
	return c.join(InnerJoin, table, onJoinCriterion, andJoinCriterion...)
}

func (c *CountDsl) JoinQ(subQuery Builder[*SubQuery], onJoinCriterion *JoinCriterion, andJoinCriterion ...*JoinCriterion) *CountDsl {
	return c.join(InnerJoin, subQuery.Build(), onJoinCriterion, andJoinCriterion...)
}

func (c *CountDsl) LeftJoin(table *Table, onJoinCriterion *JoinCriterion, andJoinCriterion ...*JoinCriterion) *CountDsl {
	return c.join(LeftJoin, table, onJoinCriterion, andJoinCriterion...)
}

func (c *CountDsl) LeftJoinQ(subQuery Builder[*SubQuery], onJoinCriterion *JoinCriterion, andJoinCriterion ...*JoinCriterion) *CountDsl {
	return c.join(LeftJoin, subQuery.Build(), onJoinCriterion, andJoinCriterion...)
}

func (c *CountDsl) RightJoin(table *Table, onJoinCriterion *JoinCriterion, andJoinCriterion ...*JoinCriterion) *CountDsl {
	return c.join(RightJoin, table, onJoinCriterion, andJoinCriterion...)
}

func (c *CountDsl) RightJoinQ(subQuery Builder[*SubQuery], onJoinCriterion *JoinCriterion, andJoinCriterion ...*JoinCriterion) *CountDsl {
	return c.join(RightJoin, subQuery.Build(), onJoinCriterion, andJoinCriterion...)
}

func (c *CountDsl) FullJoin(table *Table, onJoinCriterion *JoinCriterion, andJoinCriterion ...*JoinCriterion) *CountDsl {
	return c.join(FullJoin, table, onJoinCriterion, andJoinCriterion...)
}

func (c *CountDsl) FullJoinQ(subQuery Builder[*SubQuery], onJoinCriterion *JoinCriterion, andJoinCriterion ...*JoinCriterion) *CountDsl {
	return c.join(FullJoin, subQuery.Build(), onJoinCriterion, andJoinCriterion...)
}

func (c *CountDsl) join(joinType JoinType, joinTable ITable, onJoinCriterion *JoinCriterion, andJoinCriterion ...*JoinCriterion) *CountDsl {
	joinSpec := &JoinSpec{
		table:    joinTable,
		joinType: joinType,
	}
	joinSpec.joinCriteria = append(joinSpec.joinCriteria, onJoinCriterion)
	joinSpec.joinCriteria = append(joinSpec.joinCriteria, andJoinCriterion...)
	c.joinSpecs = append(c.joinSpecs, joinSpec)
	return c
}

func (c *CountDsl) Build() *Selection {
	qe := &QueryExpr{
		table: c.table,
	}
	if c.distinct {
		qe.selectList = append(qe.selectList, &CountDistinctAg{
			column: c.column,
			alias:  c.alias,
		})
	} else {
		qe.selectList = append(qe.selectList, &CountAg{
			column: c.column,
			alias:  c.alias,
		})
	}
	if c.whereBuilder != nil {
		qe.where = c.whereBuilder.BuildWhereModel()
	}
	if len(c.joinSpecs) > 0 {
		qe.join = &JoinMdl{
			joinSpecs: c.joinSpecs,
		}
	}
	return &Selection{
		queryExprs: []*QueryExpr{
			qe,
		},
	}
}

func (c *CountWhereBuilder) Build() *Selection {
	return c.countDsl.Build()
}

func (c *CountWhereBuilder) BuildWhereModel() *WhereMdl {
	return &WhereMdl{
		criterion:   c.firstCriterion,
		subCriteria: c.subCriteria,
	}
}

func (c *CountWhereBuilder) And(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *CountWhereBuilder {
	cri := &ColumnConditionCriterion{
		column:    column,
		condition: condition,
	}
	c.subCriteria = append(c.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return c
}

func (c *CountWhereBuilder) AndE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *CountWhereBuilder {
	cri := &ExistsCriterion{
		existsPredicate: predicate,
	}
	c.subCriteria = append(c.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return c
}

func (c *CountWhereBuilder) AndC(criterion Criterion, subCriteria ...*AndOrCriteria) *CountWhereBuilder {
	c.subCriteria = append(c.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return c
}

func (c *CountWhereBuilder) Or(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *CountWhereBuilder {
	cri := &ColumnConditionCriterion{
		column:    column,
		condition: condition,
	}
	c.subCriteria = append(c.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return c
}

func (c *CountWhereBuilder) OrE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *CountWhereBuilder {
	cri := &ExistsCriterion{
		existsPredicate: predicate,
	}
	c.subCriteria = append(c.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return c
}

func (c *CountWhereBuilder) OrC(criterion Criterion, subCriteria ...*AndOrCriteria) *CountWhereBuilder {
	c.subCriteria = append(c.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return c
}
