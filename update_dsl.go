package q

import (
	"context"
	"github.com/wxy365/basal/fn"
	"reflect"
	"strings"
)

type UpdateDsl struct {
	table        *Table
	values       []*ValueColumnMapping
	whereBuilder *UpdateWhereBuilder
	limit        int
	orderBy      *OrderByMdl
}

type ValueColumnMapping struct {
	column *Column
	value  any
}

func Update(table string) *UpdateDsl {
	return &UpdateDsl{
		table: Tbl(table),
	}
}

func UpdateDo(do DO) *UpdateDsl {
	dsl := UpdateDsl{
		table: Tbl(do.TableName()),
	}
	val := reflect.ValueOf(do)
	if val.Kind() != reflect.Pointer {
		panic("The table record object to be updated must be a pointer to a struct")
	}
	val = val.Elem()
	for val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	for i := 0; i < val.NumField(); i++ {
		if dbTag, ok := val.Type().Field(i).Tag.Lookup("db"); ok && dbTag != "" {
			col := strings.Split(dbTag, ";")[0]
			fv := val.Field(i)
			if !fv.IsZero() {
				dsl.values = append(dsl.values, &ValueColumnMapping{
					column: Col(col),
					value:  fv.Interface(),
				})
			}
		}
	}
	return &dsl
}

func SaveDo(do DO) *UpdateDsl {
	dsl := UpdateDsl{
		table: Tbl(do.TableName()),
	}
	val := reflect.ValueOf(do)
	if val.Kind() != reflect.Pointer {
		panic("The table record object to be updated must be a pointer to a struct")
	}
	val = val.Elem()
	for val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	for i := 0; i < val.NumField(); i++ {
		if dbTag, ok := val.Type().Field(i).Tag.Lookup("db"); ok && dbTag != "" {
			col := strings.Split(dbTag, ";")[0]
			dsl.values = append(dsl.values, &ValueColumnMapping{
				column: Col(col),
				value:  val.Field(i).Interface(),
			})
		}
	}
	return &dsl
}

func (u *UpdateDsl) Build() *Updating {
	if len(u.values) == 0 {
		panic("The columns to be updated are not specified.")
	}
	if u.whereBuilder == nil {
		panic("The database update operation must include filter conditions")
	}
	return &Updating{
		table:   u.table,
		where:   u.whereBuilder.BuildModel(),
		values:  u.values,
		limit:   u.limit,
		orderBy: u.orderBy,
	}
}

func (u *UpdateDsl) Set(column string) *SetClauseEnd {
	return u.SetC(Col(column))
}
func (u *UpdateDsl) SetC(column *Column) *SetClauseEnd {
	return &SetClauseEnd{
		column:    column,
		updateDsl: u,
	}
}

func (u *UpdateDsl) Limit(limit int) *UpdateDsl {
	u.limit = limit
	return u
}

func (u *UpdateDsl) OrderBy(spec ...OrderBySpec) *UpdateDsl {
	u.orderBy = &OrderByMdl{
		spec,
	}
	return u
}

func (u *UpdateDsl) Action(ctx context.Context, db *DB) error {
	renderCtx := &RenderCtx{
		dbType: db.dbType,
		cnt:    new(fn.Counter),
	}
	stm, params := u.Build().GetStatement(renderCtx).prepare()
	_, err := db.ExecContext(ctx, stm, params...)
	return err
}

func (u *UpdateDsl) ActionTx(ctx context.Context, tx *TX) error {
	renderCtx := &RenderCtx{
		dbType: tx.dbType,
		cnt:    new(fn.Counter),
	}
	stm, params := u.Build().GetStatement(renderCtx).prepare()
	_, err := tx.ExecContext(ctx, stm, params...)
	return err
}

type UpdateWhereBuilder struct {
	firstCriterion Criterion
	subCriteria    []*AndOrCriteria
	updateDsl      *UpdateDsl
}

func (u *UpdateWhereBuilder) And(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *UpdateWhereBuilder {
	cri := &ColumnConditionCriterion{
		column:    column,
		condition: condition,
	}
	u.subCriteria = append(u.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return u
}

func (u *UpdateWhereBuilder) AndE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *UpdateWhereBuilder {
	cri := &ExistsCriterion{
		existsPredicate: predicate,
	}
	u.subCriteria = append(u.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return u
}

func (u *UpdateWhereBuilder) AndC(criterion Criterion, subCriteria ...*AndOrCriteria) *UpdateWhereBuilder {
	u.subCriteria = append(u.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return u
}

func (u *UpdateWhereBuilder) Or(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *UpdateWhereBuilder {
	cri := &ColumnConditionCriterion{
		column:    column,
		condition: condition,
	}
	u.subCriteria = append(u.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return u
}

func (u *UpdateWhereBuilder) OrE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *UpdateWhereBuilder {
	cri := &ExistsCriterion{
		existsPredicate: predicate,
	}
	u.subCriteria = append(u.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return u
}

func (u *UpdateWhereBuilder) OrC(criterion Criterion, subCriteria ...*AndOrCriteria) *UpdateWhereBuilder {
	u.subCriteria = append(u.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return u
}

func (u *UpdateWhereBuilder) Limit(limit int) *UpdateDsl {
	return u.updateDsl.Limit(limit)
}

func (u *UpdateWhereBuilder) OrderBy(spec ...OrderBySpec) *UpdateDsl {
	return u.updateDsl.OrderBy(spec...)
}

func (u *UpdateWhereBuilder) BuildModel() *WhereMdl {
	return &WhereMdl{
		criterion:   u.firstCriterion,
		subCriteria: u.subCriteria,
	}
}

type SetClauseEnd struct {
	column    *Column
	updateDsl *UpdateDsl
}

func (s *SetClauseEnd) EqualTo(value any) *UpdateDsl {
	s.updateDsl.values = append(s.updateDsl.values, &ValueColumnMapping{
		column: s.column,
		value:  value,
	})
	return s.updateDsl
}
