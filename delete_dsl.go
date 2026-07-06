package q

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
