package q

import (
	"context"

	"github.com/wxy365/basal/fn"
)

// Delete creates a new DELETE query builder.
func Delete() *DeleteFromDsl {
	return &DeleteFromDsl{}
}

// DeleteFromDsl is the DSL state after Delete(), before From() is called.
type DeleteFromDsl struct{}

// From specifies the table to delete from.
func (d *DeleteFromDsl) From(table string) *DeleteWhereBuilder {
	return &DeleteWhereBuilder{
		deleteDsl: &DeleteDsl{table: Tbl(table)},
	}
}

// DeleteDsl holds the internal state for a DELETE query.
type DeleteDsl struct {
	table   *Table
	where   *WhereMdl
	orderBy *OrderByMdl
	limit   int
}

// Limit sets a LIMIT clause on the DELETE query.
func (d *DeleteDsl) Limit(limit int) *DeleteDsl {
	d.limit = limit
	return d
}

// Build finalizes the DELETE query and returns a Deletion model.
func (d *DeleteDsl) Build() *Deletion {
	return &Deletion{
		table:   d.table,
		where:   d.where,
		orderBy: d.orderBy,
		limit:   d.limit,
	}
}

// Action executes the DELETE query against the default data source or an
// optional specified DB.
func (d *DeleteDsl) Action(ctx context.Context, optDb ...*DB) error {
	db, err := resolveDB(optDb...)
	if err != nil {
		return err
	}
	renderCtx := &RenderCtx{
		dbType: db.dbType,
		cnt:    new(fn.Counter),
	}
	stm, params := d.Build().GetStatement(renderCtx).prepare()
	_, err = db.ExecContext(ctx, stm, params...)
	return err
}

// ActionTx executes the DELETE query within a given transaction.
func (d *DeleteDsl) ActionTx(ctx context.Context, tx *TX) error {
	tx, err := resolveTx(tx)
	if err != nil {
		return err
	}
	renderCtx := &RenderCtx{
		dbType: tx.dbType,
		cnt:    new(fn.Counter),
	}
	stm, params := d.Build().GetStatement(renderCtx).prepare()
	_, err = tx.ExecContext(ctx, stm, params...)
	return err
}

// DeleteWhereBuilder builds the WHERE clause for a DELETE query.
type DeleteWhereBuilder struct {
	deleteDsl      *DeleteDsl
	firstCriterion Criterion
	subCriteria    []*AndOrCriteria
}

// Where sets the initial WHERE condition.
func (d *DeleteWhereBuilder) Where(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *DeleteWhereBuilder {
	d.firstCriterion = &ColumnConditionCriterion{
		column:    column,
		condition: condition,
	}
	d.subCriteria = subCriteria
	return d
}

// And adds an AND condition.
func (d *DeleteWhereBuilder) And(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *DeleteWhereBuilder {
	cri := &ColumnConditionCriterion{
		column:    column,
		condition: condition,
	}
	d.subCriteria = append(d.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return d
}

// Or adds an OR condition.
func (d *DeleteWhereBuilder) Or(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *DeleteWhereBuilder {
	cri := &ColumnConditionCriterion{
		column:    column,
		condition: condition,
	}
	d.subCriteria = append(d.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return d
}

// AndE adds an AND EXISTS condition.
func (d *DeleteWhereBuilder) AndE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *DeleteWhereBuilder {
	cri := &ExistsCriterion{
		existsPredicate: predicate,
	}
	d.subCriteria = append(d.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return d
}

// AndC adds an AND condition using a raw Criterion value.
func (d *DeleteWhereBuilder) AndC(criterion Criterion, subCriteria ...*AndOrCriteria) *DeleteWhereBuilder {
	d.subCriteria = append(d.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return d
}

// OrE adds an OR EXISTS condition.
func (d *DeleteWhereBuilder) OrE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *DeleteWhereBuilder {
	cri := &ExistsCriterion{
		existsPredicate: predicate,
	}
	d.subCriteria = append(d.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return d
}

// OrC adds an OR condition using a raw Criterion value.
func (d *DeleteWhereBuilder) OrC(criterion Criterion, subCriteria ...*AndOrCriteria) *DeleteWhereBuilder {
	d.subCriteria = append(d.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return d
}

// Action executes the DELETE query against the default data source or an
// optional specified DB.
func (d *DeleteWhereBuilder) Action(ctx context.Context, optDb ...*DB) error {
	d.flushWhere()
	return d.deleteDsl.Action(ctx, optDb...)
}

// ActionTx executes the DELETE query within a given transaction.
func (d *DeleteWhereBuilder) ActionTx(ctx context.Context, tx *TX) error {
	d.flushWhere()
	return d.deleteDsl.ActionTx(ctx, tx)
}

// OrderBy adds an ORDER BY clause.
func (d *DeleteWhereBuilder) OrderBy(spec ...OrderBySpec) *DeleteDsl {
	d.flushWhere()
	d.deleteDsl.orderBy = &OrderByMdl{
		columns: spec,
	}
	return d.deleteDsl
}

// Limit sets a LIMIT clause.
func (d *DeleteWhereBuilder) Limit(limit int) *DeleteDsl {
	d.flushWhere()
	d.deleteDsl.limit = limit
	return d.deleteDsl
}

// Build finalizes the DELETE query and returns a Deletion model.
func (d *DeleteWhereBuilder) Build() *Deletion {
	d.flushWhere()
	return d.deleteDsl.Build()
}

func (d *DeleteWhereBuilder) flushWhere() {
	if d.firstCriterion != nil {
		d.deleteDsl.where = &WhereMdl{
			criterion:   d.firstCriterion,
			subCriteria: d.subCriteria,
		}
	}
}
