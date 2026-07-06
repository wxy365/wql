package q

import (
	"context"
	"database/sql"
	"reflect"
	"strings"

	"github.com/wxy365/basal/errs"
	"github.com/wxy365/basal/fn"
)

// SelectQueryExprDsl is the DSL builder for a SELECT query expression.
// It provides methods to specify WHERE, GROUP BY, HAVING, ORDER BY, JOIN,
// UNION, LIMIT and OFFSET clauses for a single SELECT statement.
type SelectQueryExprDsl struct {
	connector     string
	selectDsl     *SelectDsl
	distinct      bool
	selectList    []IColumn
	whereBuilder  *QueryExprWhereBuilder
	groupBy       *GroupByMdl
	havingBuilder *QueryExprHavingBuilder
	table         ITable
	joinSpecs     []*JoinSpec
}

// Wheres starts building a WHERE clause for this SELECT query expression,
// returning a builder that accepts chained AND/OR conditions.
func (s *SelectQueryExprDsl) Wheres() *QueryExprWhereBuilder {
	b := &QueryExprWhereBuilder{
		queryExprDsl: s,
	}
	s.whereBuilder = b
	return b
}

// Where sets the initial WHERE criterion on this SELECT query expression
// using a column and a Condition, optionally followed by sub-criteria.
func (s *SelectQueryExprDsl) Where(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	b := &QueryExprWhereBuilder{
		queryExprDsl: s,
		firstCriterion: &ColumnConditionCriterion{
			column:    column,
			condition: condition,
		},
		subCriteria: subCriteria,
	}
	s.whereBuilder = b
	return b
}

// WhereE sets the initial WHERE criterion using an EXISTS predicate,
// optionally followed by sub-criteria.
func (s *SelectQueryExprDsl) WhereE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	b := &QueryExprWhereBuilder{
		queryExprDsl: s,
		firstCriterion: &ExistsCriterion{
			existsPredicate: predicate,
		},
		subCriteria: subCriteria,
	}
	s.whereBuilder = b
	return b
}

// WhereC sets the initial WHERE criterion directly as a Criterion value,
// optionally followed by sub-criteria.
func (s *SelectQueryExprDsl) WhereC(criterion Criterion, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	b := &QueryExprWhereBuilder{
		queryExprDsl:   s,
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	}
	s.whereBuilder = b
	return b
}

// GroupBy adds a GROUP BY clause to this SELECT query expression using
// column name strings.
func (s *SelectQueryExprDsl) GroupBy(columns ...string) *GroupByEnd {
	var cols []IColumn
	for _, col := range columns {
		cols = append(cols, Col(col))
	}
	return s.GroupByC(cols...)
}

// GroupByC adds a GROUP BY clause to this SELECT query expression using
// IColumn references.
func (s *SelectQueryExprDsl) GroupByC(columns ...IColumn) *GroupByEnd {
	s.groupBy = &GroupByMdl{
		columns: columns,
	}
	return &GroupByEnd{
		queryExprDsl: s,
	}
}

// Limit sets a LIMIT clause on this SELECT query expression.
func (s *SelectQueryExprDsl) Limit(limit int) *LimitEnd {
	return s.selectDsl.Limit(limit)
}

// Offset sets an OFFSET clause on this SELECT query expression.
func (s *SelectQueryExprDsl) Offset(offset int) *OffsetEnd {
	return s.selectDsl.Offset(offset)
}

// OrderBy adds an ORDER BY clause to this SELECT query expression.
func (s *SelectQueryExprDsl) OrderBy(spec ...OrderBySpec) *SelectDsl {
	s.selectDsl.OrderBy(spec...)
	return s.selectDsl
}

// Union starts building a UNION with another SELECT statement.
func (s *SelectQueryExprDsl) Union() *UnionBuilder {
	return &UnionBuilder{
		queryExprDsl: s,
		connector:    "union",
	}
}

// UnionAll starts building a UNION ALL with another SELECT statement.
func (s *SelectQueryExprDsl) UnionAll() *UnionBuilder {
	return &UnionBuilder{
		queryExprDsl: s,
		connector:    "union all",
	}
}

// Havings starts building a HAVING clause for this SELECT query expression.
func (s *SelectQueryExprDsl) Havings() *QueryExprHavingBuilder {
	if s.havingBuilder == nil {
		s.havingBuilder = &QueryExprHavingBuilder{
			queryExprDsl: s,
		}
	}
	return s.havingBuilder
}

// Join adds an INNER JOIN with a Table.
func (s *SelectQueryExprDsl) Join(table *Table) *JoinSpecStart {
	return s.join(table, InnerJoin)
}

// JoinQ adds an INNER JOIN with a SubQuery.
func (s *SelectQueryExprDsl) JoinQ(builder Builder[*SubQuery]) *JoinSpecStart {
	return s.join(builder.Build(), InnerJoin)
}

// LeftJoin adds a LEFT JOIN with a Table.
func (s *SelectQueryExprDsl) LeftJoin(table *Table) *JoinSpecStart {
	return s.join(table, LeftJoin)
}

// LeftJoinQ adds a LEFT JOIN with a SubQuery.
func (s *SelectQueryExprDsl) LeftJoinQ(builder Builder[*SubQuery]) *JoinSpecStart {
	return s.join(builder.Build(), LeftJoin)
}

// RightJoin adds a RIGHT JOIN with a Table.
func (s *SelectQueryExprDsl) RightJoin(table *Table) *JoinSpecStart {
	return s.join(table, RightJoin)
}

// RightJoinQ adds a RIGHT JOIN with a SubQuery.
func (s *SelectQueryExprDsl) RightJoinQ(builder Builder[*SubQuery]) *JoinSpecStart {
	return s.join(builder.Build(), RightJoin)
}

// FullJoin adds a FULL JOIN with a Table.
func (s *SelectQueryExprDsl) FullJoin(table *Table) *JoinSpecStart {
	return s.join(table, FullJoin)
}

// FullJoinQ adds a FULL JOIN with a SubQuery.
func (s *SelectQueryExprDsl) FullJoinQ(builder Builder[*SubQuery]) *JoinSpecStart {
	return s.join(builder.Build(), FullJoin)
}

func (s *SelectQueryExprDsl) join(table ITable, joinType JoinType) *JoinSpecStart {
	return &JoinSpecStart{
		queryExprDsl: s,
		joinTable:    table,
		joinType:     joinType,
	}
}

// BuildModel constructs the internal QueryExpr model from this DSL builder.
func (s *SelectQueryExprDsl) BuildModel() *QueryExpr {
	qe := &QueryExpr{
		connector:  s.connector,
		selectList: s.selectList,
		table:      s.table,
		distinct:   s.distinct,
		groupBy:    s.groupBy,
	}
	if s.whereBuilder != nil {
		qe.where = s.whereBuilder.BuildModel()
	}
	if len(s.joinSpecs) > 0 {
		qe.join = &JoinMdl{s.joinSpecs}
	}
	if s.havingBuilder != nil {
		qe.having = s.havingBuilder.BuildModel()
	}
	return qe
}

// Build finalizes this DSL and returns the resulting Selection.
func (s *SelectQueryExprDsl) Build() *Selection {
	return s.selectDsl.Build()
}

// Action executes the SELECT query directly against the default data source
// or an optional specified DB, scanning results into the target if configured.
func (s *SelectQueryExprDsl) Action(ctx context.Context, optDb ...*DB) error {
	return s.selectDsl.Action(ctx, optDb...)
}

// ActionTx executes the SELECT query within a given transaction.
func (s *SelectQueryExprDsl) ActionTx(ctx context.Context, tx *TX) error {
	return s.selectDsl.ActionTx(ctx, tx)
}

// As wraps this SELECT query as a sub-query with the given alias.
func (s *SelectQueryExprDsl) As(alias string) *SubQueryDsl {
	return s.selectDsl.As(alias)
}

// JoinSpecStart is the DSL state after specifying a JOIN target, before
// the ON condition is provided.
type JoinSpecStart struct {
	joinTable    ITable
	joinType     JoinType
	queryExprDsl *SelectQueryExprDsl
}

// On specifies the JOIN condition through a column name and a
// JoinCondition (e.g. EqualTo, EqualToValue), with optional AND criteria.
func (j *JoinSpecStart) On(column string, condition JoinCondition, andCriteria ...*JoinCriterion) *JoinSpecEnd {
	return j.OnC(Col(column), condition, andCriteria...)
}

// OnC specifies the JOIN condition through an IColumn reference and a
// JoinCondition, with optional AND criteria.
func (j *JoinSpecStart) OnC(column IColumn, condition JoinCondition, andCriteria ...*JoinCriterion) *JoinSpecEnd {
	joinCriterion := &JoinCriterion{
		connector:  "on",
		leftColumn: column,
		condition:  condition,
	}
	spec := &JoinSpec{
		table:    j.joinTable,
		joinType: j.joinType,
	}
	spec.joinCriteria = append(spec.joinCriteria, joinCriterion)
	spec.joinCriteria = append(spec.joinCriteria, andCriteria...)
	j.queryExprDsl.joinSpecs = append(j.queryExprDsl.joinSpecs, spec)
	return &JoinSpecEnd{
		joinSpec:     spec,
		queryExprDsl: j.queryExprDsl,
	}
}

// JoinSpecEnd is the DSL state after a JOIN ON condition has been
// specified, allowing further AND criteria, additional JOINs, or WHERE.
type JoinSpecEnd struct {
	joinSpec     *JoinSpec
	queryExprDsl *SelectQueryExprDsl
}

// Build finalizes the query and returns the Selection.
func (j *JoinSpecEnd) Build() *Selection {
	return j.queryExprDsl.Build()
}

// Wheres starts building a WHERE clause for this query.
func (j *JoinSpecEnd) Wheres() *QueryExprWhereBuilder {
	return j.queryExprDsl.Wheres()
}

// And adds an additional AND condition to this JOIN's ON clause.
func (j *JoinSpecEnd) And(column IColumn, condition JoinCondition) *JoinSpecEnd {
	joinCriterion := &JoinCriterion{
		connector:  "and",
		leftColumn: column,
		condition:  condition,
	}
	j.joinSpec.joinCriteria = append(j.joinSpec.joinCriteria, joinCriterion)
	return j
}

// Join adds an additional INNER JOIN to this query.
func (j *JoinSpecEnd) Join(table *Table) *JoinSpecStart {
	return j.queryExprDsl.Join(table)
}

// JoinQ adds an additional INNER JOIN with a SubQuery.
func (j *JoinSpecEnd) JoinQ(builder Builder[*SubQuery]) *JoinSpecStart {
	return j.queryExprDsl.JoinQ(builder)
}

// Select starts building a SELECT query with the given column name strings.
// Use From (or FromT / FromDO) to specify the table and continue the DSL chain.
func Select(columns ...string) *FromGather {
	fg := new(FromGather)
	for _, column := range columns {
		fg.selectList = append(fg.selectList, Col(column))
	}
	fg.selectDsl = new(SelectDsl)
	return fg
}

// SelectDistinct starts building a SELECT DISTINCT query with the given
// column name strings.
func SelectDistinct(columns ...string) *FromGather {
	fg := Select(columns...)
	fg.distinct = true
	return fg
}

// SelectC starts building a SELECT query with the given IColumn references.
func SelectC(columns ...IColumn) *FromGather {
	fg := new(FromGather)
	for _, column := range columns {
		fg.selectList = append(fg.selectList, column)
	}
	fg.selectDsl = new(SelectDsl)
	return fg
}

// SelectDistinctC starts building a SELECT DISTINCT query with the given
// IColumn references.
func SelectDistinctC(columns ...IColumn) *FromGather {
	fg := SelectC(columns...)
	fg.distinct = true
	return fg
}

// SelectDsl is the top-level DSL builder for a SELECT query. It aggregates
// one or more SelectQueryExprDsl entries (for UNION support) and tracks
// ORDER BY, LIMIT and OFFSET clauses at the query level.
type SelectDsl struct {
	queryExprs []*SelectQueryExprDsl
	orderBy    *OrderByMdl
	limit      int
	offset     int

	target any
}

// Limit sets a LIMIT clause on this SELECT query.
func (s *SelectDsl) Limit(limit int) *LimitEnd {
	s.limit = limit
	return &LimitEnd{
		selectDsl: s,
	}
}

// Offset sets an OFFSET clause on this SELECT query.
func (s *SelectDsl) Offset(offset int) *OffsetEnd {
	s.offset = offset
	return &OffsetEnd{
		selectDsl: s,
	}
}

// OrderBy adds an ORDER BY clause to this SELECT query.
func (s *SelectDsl) OrderBy(spec ...OrderBySpec) {
	s.orderBy = &OrderByMdl{
		columns: spec,
	}
}

// Build finalizes the DSL and returns a Selection model.
func (s *SelectDsl) Build() *Selection {
	var qe []*QueryExpr
	for _, qeDsl := range s.queryExprs {
		qe = append(qe, qeDsl.BuildModel())
	}
	res := &Selection{
		queryExprs: qe,
		orderBy:    s.orderBy,
	}
	if s.limit > 0 || s.offset > 0 {
		res.paging = &PagingMdl{
			limit:  s.limit,
			offset: s.offset,
		}
	}
	return res
}

// As wraps this SELECT query as a sub-query with the given alias.
func (s *SelectDsl) As(alias string) *SubQueryDsl {
	return &SubQueryDsl{
		selectDsl: s,
		alias:     alias,
	}
}

// Into sets the target struct pointer (a DO) for scanning query results.
func (s *SelectDsl) Into(target any) *SelectDsl {
	s.target = target
	return s
}

// Action executes the SELECT query against the default data source or an
// optional specified DB, scanning results into the target if configured.
func (s *SelectDsl) Action(ctx context.Context, optDb ...*DB) error {
	db := DataSource
	if len(optDb) > 0 {
		db = optDb[0]
	}
	renderCtx := &RenderCtx{
		dbType: db.dbType,
		cnt:    new(fn.Counter),
	}
	stm, args := s.Build().GetStatement(renderCtx).prepare()
	rows, err := db.QueryContext(ctx, stm, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return s.readRows(rows)
}

// ActionTx executes the SELECT query within a given transaction.
func (s *SelectDsl) ActionTx(ctx context.Context, tx *TX) error {
	renderCtx := &RenderCtx{
		dbType: tx.dbType,
		cnt:    new(fn.Counter),
	}
	stm, args := s.Build().GetStatement(renderCtx).prepare()
	rows, err := tx.QueryContext(ctx, stm, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return s.readRows(rows)
}

func (s *SelectDsl) readRows(rows *sql.Rows) error {
	if s.target == nil {
		s.target = rows
		return nil
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	doVal := reflect.ValueOf(s.target)
	if doVal.Kind() != reflect.Pointer {
		return errs.New("The target is supposed to be of pointer kind")
	}
	doVal = doVal.Elem()
	for doVal.Kind() == reflect.Pointer {
		doVal = doVal.Elem()
	}
	// pre-build column-to-field-index map
	colIndexes := make([]int, len(columns))
	for ci, column := range columns {
		colIndexes[ci] = -1
		for i := 0; i < doVal.NumField(); i++ {
			if col, ok := doVal.Type().Field(i).Tag.Lookup("db"); ok && (col == column || strings.Split(col, ";")[0] == column) {
				colIndexes[ci] = i
				break
			}
		}
	}
	for rows.Next() {
		fields := make([]any, len(columns))
		for ci, fi := range colIndexes {
			if fi >= 0 {
				fields[ci] = doVal.Field(fi).Addr().Interface()
			} else {
				var placeholder any
				fields[ci] = &placeholder
			}
		}
		if err = rows.Scan(fields...); err != nil {
			return err
		}
	}
	return nil
}

// SubQueryDsl wraps a completed SELECT query as a sub-query with an alias.
type SubQueryDsl struct {
	selectDsl *SelectDsl
	alias     string
}

// Build finalizes the sub-query and returns a SubQuery model.
func (s *SubQueryDsl) Build() *SubQuery {
	return &SubQuery{
		selection: s.selectDsl.Build(),
		alias:     s.alias,
	}
}

// FromGather holds the SELECT column list before the FROM clause is
// specified. Call From, FromT, FromQ or FromDO to continue the DSL chain.
type FromGather struct {
	connector  string
	selectList []IColumn
	distinct   bool
	selectDsl  *SelectDsl
}

// From specifies the table name for the SELECT query and returns the
// query expression builder for further clause chaining.
func (f *FromGather) From(table string) *SelectQueryExprDsl {
	return f.FromT(Tbl(table))
}

// FromT specifies the table as an ITable reference for the SELECT query.
func (f *FromGather) FromT(table ITable) *SelectQueryExprDsl {
	for _, col := range f.selectList {
		if ns := col.namespace(); !ns.IsPresent() {
			col.setNamespace(table.Alias().OrElse(table.Name()))
		}
	}
	queryExpr := &SelectQueryExprDsl{
		selectDsl:  f.selectDsl,
		connector:  f.connector,
		selectList: f.selectList,
		distinct:   f.distinct,
		table:      table,
	}
	f.selectDsl.queryExprs = append(f.selectDsl.queryExprs, queryExpr)
	return queryExpr
}

// FromQ specifies a sub-query as the data source for the SELECT.
func (f *FromGather) FromQ(sb Builder[*SubQuery]) *SelectQueryExprDsl {
	table := sb.Build()
	return f.FromT(table)
}

// FromDO specifies a DO (Domain Object) as the data source, deriving the
// table name from the DO and setting it as the result target.
func (f *FromGather) FromDO(do DO) *SelectQueryExprDsl {
	f.selectDsl.Into(do)
	return f.From(do.TableName())
}

// QueryExprWhereBuilder builds the WHERE clause for a SELECT query
// expression, supporting chained AND/OR conditions.
type QueryExprWhereBuilder struct {
	firstCriterion Criterion
	subCriteria    []*AndOrCriteria
	queryExprDsl   *SelectQueryExprDsl
}

// Build finalizes the query and returns the Selection.
func (q *QueryExprWhereBuilder) Build() *Selection {
	return q.queryExprDsl.Build()
}

// Action executes the SELECT query against the default data source.
func (q *QueryExprWhereBuilder) Action(ctx context.Context, optDb ...*DB) error {
	return q.queryExprDsl.Action(ctx, optDb...)
}

// ActionTx executes the SELECT query within a given transaction.
func (q *QueryExprWhereBuilder) ActionTx(ctx context.Context, tx *TX) error {
	return q.queryExprDsl.ActionTx(ctx, tx)
}

// BuildModel constructs the internal WhereMdl from the WHERE builder state.
func (q *QueryExprWhereBuilder) BuildModel() *WhereMdl {
	return &WhereMdl{
		criterion:   q.firstCriterion,
		subCriteria: q.subCriteria,
	}
}

// And adds an AND criterion to the WHERE clause.
func (q *QueryExprWhereBuilder) And(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	cri := &ColumnConditionCriterion{
		column:    column,
		condition: condition,
	}
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return q
}

// AndE adds an AND EXISTS criterion to the WHERE clause.
func (q *QueryExprWhereBuilder) AndE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	cri := &ExistsCriterion{
		existsPredicate: predicate,
	}
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return q
}

// AndC adds an AND criterion using a raw Criterion value.
func (q *QueryExprWhereBuilder) AndC(criterion Criterion, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return q
}

// Or adds an OR criterion to the WHERE clause.
func (q *QueryExprWhereBuilder) Or(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	cri := &ColumnConditionCriterion{
		column:    column,
		condition: condition,
	}
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return q
}

// OrE adds an OR EXISTS criterion to the WHERE clause.
func (q *QueryExprWhereBuilder) OrE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	cri := &ExistsCriterion{
		existsPredicate: predicate,
	}
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return q
}

// OrC adds an OR criterion using a raw Criterion value.
func (q *QueryExprWhereBuilder) OrC(criterion Criterion, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return q
}

// Union starts building a UNION with another SELECT statement.
func (q *QueryExprWhereBuilder) Union() *UnionBuilder {
	return &UnionBuilder{
		queryExprDsl: q.queryExprDsl,
		connector:    "union",
	}
}

// UnionAll starts building a UNION ALL with another SELECT statement.
func (q *QueryExprWhereBuilder) UnionAll() *UnionBuilder {
	return &UnionBuilder{
		queryExprDsl: q.queryExprDsl,
		connector:    "union all",
	}
}

// OrderBy adds an ORDER BY clause to the query.
func (q *QueryExprWhereBuilder) OrderBy(columns ...OrderBySpec) *SelectDsl {
	q.queryExprDsl.selectDsl.OrderBy(columns...)
	return q.queryExprDsl.selectDsl
}

// GroupBy adds a GROUP BY clause using column name strings.
func (q *QueryExprWhereBuilder) GroupBy(columns ...string) *GroupByEnd {
	return q.queryExprDsl.GroupBy(columns...)
}

// GroupByC adds a GROUP BY clause using IColumn references.
func (q *QueryExprWhereBuilder) GroupByC(columns ...IColumn) *GroupByEnd {
	return q.queryExprDsl.GroupByC(columns...)
}

// Limit sets a LIMIT clause on the query.
func (q *QueryExprWhereBuilder) Limit(limit int) *LimitEnd {
	return q.queryExprDsl.Limit(limit)
}

// Offset sets an OFFSET clause on the query.
func (q *QueryExprWhereBuilder) Offset(offset int) *OffsetEnd {
	return q.queryExprDsl.Offset(offset)
}

// QueryExprHavingBuilder builds the HAVING clause for a grouped SELECT
// query expression, supporting chained AND/OR conditions.
type QueryExprHavingBuilder struct {
	queryExprDsl   *SelectQueryExprDsl
	firstCriterion Criterion
	subCriteria    []*AndOrCriteria
}

// And adds an AND criterion to the HAVING clause.
func (q *QueryExprHavingBuilder) And(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *QueryExprHavingBuilder {
	cri := &ColumnConditionCriterion{
		column:    column,
		condition: condition,
	}
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return q
}

// AndE adds an AND EXISTS criterion to the HAVING clause.
func (q *QueryExprHavingBuilder) AndE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *QueryExprHavingBuilder {
	cri := &ExistsCriterion{
		existsPredicate: predicate,
	}
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return q
}

// AndC adds an AND criterion using a raw Criterion value to the HAVING clause.
func (q *QueryExprHavingBuilder) AndC(criterion Criterion, subCriteria ...*AndOrCriteria) *QueryExprHavingBuilder {
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return q
}

// Or adds an OR criterion to the HAVING clause.
func (q *QueryExprHavingBuilder) Or(column IColumn, condition Condition, subCriteria ...*AndOrCriteria) *QueryExprHavingBuilder {
	cri := &ColumnConditionCriterion{
		column:    column,
		condition: condition,
	}
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return q
}

// OrE adds an OR EXISTS criterion to the HAVING clause.
func (q *QueryExprHavingBuilder) OrE(predicate *ExistsPredicate, subCriteria ...*AndOrCriteria) *QueryExprHavingBuilder {
	cri := &ExistsCriterion{
		existsPredicate: predicate,
	}
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: cri,
		subCriteria:    subCriteria,
	})
	return q
}

// OrC adds an OR criterion using a raw Criterion value to the HAVING clause.
func (q *QueryExprHavingBuilder) OrC(criterion Criterion, subCriteria ...*AndOrCriteria) *QueryExprHavingBuilder {
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return q
}

// BuildModel constructs the internal HavingMdl from the HAVING builder state.
func (q *QueryExprHavingBuilder) BuildModel() *HavingMdl {
	return &HavingMdl{
		criterion:   q.firstCriterion,
		subCriteria: q.subCriteria,
	}
}

// Offset sets an OFFSET clause on the query.
func (q *QueryExprHavingBuilder) Offset(offset int) *OffsetEnd {
	return q.queryExprDsl.Offset(offset)
}

// Limit sets a LIMIT clause on the query.
func (q *QueryExprHavingBuilder) Limit(limit int) *LimitEnd {
	return q.queryExprDsl.Limit(limit)
}

// OrderBy adds an ORDER BY clause to the query.
func (q *QueryExprHavingBuilder) OrderBy(spec ...OrderBySpec) *SelectDsl {
	return q.queryExprDsl.OrderBy(spec...)
}

// Union starts building a UNION with another SELECT statement.
func (q *QueryExprHavingBuilder) Union() *UnionBuilder {
	return q.queryExprDsl.Union()
}

// UnionAll starts building a UNION ALL with another SELECT statement.
func (q *QueryExprHavingBuilder) UnionAll() *UnionBuilder {
	return q.queryExprDsl.UnionAll()
}

// Build finalizes the query and returns the Selection.
func (q *QueryExprHavingBuilder) Build() *Selection {
	return q.queryExprDsl.Build()
}

// UnionBuilder starts building a UNION or UNION ALL clause by chaining
// another SELECT statement to the current query expression.
type UnionBuilder struct {
	queryExprDsl *SelectQueryExprDsl
	connector    string
}

// Select specifies the columns for the UNION query expression.
func (u *UnionBuilder) Select(columns ...IColumn) *FromGather {
	res := &FromGather{
		connector: u.connector,
		selectDsl: u.queryExprDsl.selectDsl,
		distinct:  false,
	}
	res.selectList = append(res.selectList, columns...)
	return res
}

// SelectDistinct specifies the columns for a DISTINCT UNION query expression.
func (u *UnionBuilder) SelectDistinct(columns ...IColumn) *FromGather {
	res := &FromGather{
		connector: u.connector,
		selectDsl: u.queryExprDsl.selectDsl,
		distinct:  true,
	}
	res.selectList = append(res.selectList, columns...)
	return res
}

// GroupByEnd is the DSL state after a GROUP BY clause has been specified,
// allowing optional HAVING, ORDER BY, LIMIT, OFFSET, UNION or terminal
// operations (Build, Action, ActionTx).
type GroupByEnd struct {
	queryExprDsl *SelectQueryExprDsl
}

// Action executes the query against the default data source.
func (g *GroupByEnd) Action(ctx context.Context, optDb ...*DB) error {
	return g.queryExprDsl.Action(ctx, optDb...)
}

// ActionTx executes the query within a given transaction.
func (g *GroupByEnd) ActionTx(ctx context.Context, tx *TX) error {
	return g.queryExprDsl.ActionTx(ctx, tx)
}

// Having adds a HAVING clause to the grouped query using a column and condition.
func (g *GroupByEnd) Having(column IColumn, cond Condition, subCriteria ...*AndOrCriteria) *QueryExprHavingBuilder {
	criterion := &ColumnConditionCriterion{
		column:    column,
		condition: cond,
	}
	criterion.subCriteria = append(criterion.subCriteria, subCriteria...)
	having := g.Havings()
	having.firstCriterion = criterion
	return having
}

// HavingC adds a HAVING clause using a raw Criterion value.
func (g *GroupByEnd) HavingC(firstCriterion Criterion, subCriteria ...*AndOrCriteria) *QueryExprHavingBuilder {
	first := CriterionGroup{
		BaseCriterion: BaseCriterion{
			subCriteria: subCriteria,
		},
		firstCriterion: firstCriterion,
	}
	having := g.Havings()
	having.firstCriterion = &first
	return having
}

// Havings starts building a HAVING clause.
func (g *GroupByEnd) Havings() *QueryExprHavingBuilder {
	return g.queryExprDsl.Havings()
}

// OrderBy adds an ORDER BY clause to the query.
func (g *GroupByEnd) OrderBy(spec ...OrderBySpec) *SelectDsl {
	return g.queryExprDsl.OrderBy(spec...)
}

// As wraps this grouped query as a sub-query with the given alias.
func (g *GroupByEnd) As(alias string) *SubQueryDsl {
	return g.queryExprDsl.selectDsl.As(alias)
}

// Build finalizes the query and returns the Selection.
func (g *GroupByEnd) Build() *Selection {
	return g.queryExprDsl.selectDsl.Build()
}

// Union starts building a UNION with another SELECT statement.
func (g *GroupByEnd) Union() *UnionBuilder {
	return g.queryExprDsl.Union()
}

// UnionAll starts building a UNION ALL with another SELECT statement.
func (g *GroupByEnd) UnionAll() *UnionBuilder {
	return g.queryExprDsl.UnionAll()
}

// Limit sets a LIMIT clause on the query.
func (g *GroupByEnd) Limit(limit int) *LimitEnd {
	return g.queryExprDsl.Limit(limit)
}

// Offset sets an OFFSET clause on the query.
func (g *GroupByEnd) Offset(offset int) *OffsetEnd {
	return g.queryExprDsl.Offset(offset)
}

// LimitEnd is the DSL state after a LIMIT clause has been specified,
// allowing optional OFFSET, Build, Action, ActionTx or wrapping as a sub-query.
type LimitEnd struct {
	selectDsl *SelectDsl
}

// Action executes the query against the default data source.
func (l *LimitEnd) Action(ctx context.Context, optDb ...*DB) error {
	return l.selectDsl.Action(ctx, optDb...)
}

// ActionTx executes the query within a given transaction.
func (l *LimitEnd) ActionTx(ctx context.Context, tx *TX) error {
	return l.selectDsl.ActionTx(ctx, tx)
}

// Offset sets an OFFSET clause on the query.
func (l *LimitEnd) Offset(offset int) *OffsetEnd {
	return l.selectDsl.Offset(offset)
}

// Build finalizes the query and returns the Selection.
func (l *LimitEnd) Build() *Selection {
	return l.selectDsl.Build()
}

// As wraps this query as a sub-query with the given alias.
func (l *LimitEnd) As(alias string) *SubQueryDsl {
	return l.selectDsl.As(alias)
}

// OffsetEnd is the DSL state after an OFFSET clause has been specified,
// allowing terminal operations (Build, Action, ActionTx) or wrapping as a sub-query.
type OffsetEnd struct {
	selectDsl *SelectDsl
}

// Action executes the query against the default data source.
func (o *OffsetEnd) Action(ctx context.Context, optDb ...*DB) error {
	return o.selectDsl.Action(ctx, optDb...)
}

// ActionTx executes the query within a given transaction.
func (o *OffsetEnd) ActionTx(ctx context.Context, tx *TX) error {
	return o.selectDsl.ActionTx(ctx, tx)
}

// Build finalizes the query and returns the Selection.
func (o *OffsetEnd) Build() *Selection {
	return o.selectDsl.Build()
}

// As wraps this query as a sub-query with the given alias.
func (o *OffsetEnd) As(alias string) *SubQueryDsl {
	return o.selectDsl.As(alias)
}
