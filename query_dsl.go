package q

import (
	"context"
	"database/sql"
	"github.com/wxy365/basal/fn"
	"github.com/wxy365/basal/lei"
	"reflect"
	"strings"
)

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

func (s *SelectQueryExprDsl) Wheres() *QueryExprWhereBuilder {
	b := &QueryExprWhereBuilder{
		queryExprDsl: s,
	}
	s.whereBuilder = b
	return b
}

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

func (s *SelectQueryExprDsl) WhereC(criterion Criterion, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	b := &QueryExprWhereBuilder{
		queryExprDsl:   s,
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	}
	s.whereBuilder = b
	return b
}

func (s *SelectQueryExprDsl) GroupBy(columns ...string) *GroupByEnd {
	var cols []IColumn
	for _, col := range columns {
		cols = append(cols, Col(col))
	}
	return s.GroupByC(cols...)
}

func (s *SelectQueryExprDsl) GroupByC(columns ...IColumn) *GroupByEnd {
	s.groupBy = &GroupByMdl{
		columns: columns,
	}
	return &GroupByEnd{
		queryExprDsl: s,
	}
}

func (s *SelectQueryExprDsl) Limit(limit int) *LimitEnd {
	return s.selectDsl.Limit(limit)
}

func (s *SelectQueryExprDsl) Offset(offset int) *OffsetEnd {
	return s.selectDsl.Offset(offset)
}

func (s *SelectQueryExprDsl) OrderBy(spec ...OrderBySpec) *SelectDsl {
	s.selectDsl.OrderBy(spec...)
	return s.selectDsl
}

func (s *SelectQueryExprDsl) Union() *UnionBuilder {
	return &UnionBuilder{
		queryExprDsl: s,
		connector:    "union",
	}
}

func (s *SelectQueryExprDsl) UnionAll() *UnionBuilder {
	return &UnionBuilder{
		queryExprDsl: s,
		connector:    "union all",
	}
}

func (s *SelectQueryExprDsl) Havings() *QueryExprHavingBuilder {
	if s.havingBuilder == nil {
		s.havingBuilder = &QueryExprHavingBuilder{
			queryExprDsl: s,
		}
	}
	return s.havingBuilder
}

func (s *SelectQueryExprDsl) Join(table *Table) *JoinSpecStart {
	return s.join(table, InnerJoin)
}

func (s *SelectQueryExprDsl) JoinQ(builder Builder[*SubQuery]) *JoinSpecStart {
	return s.join(builder.Build(), InnerJoin)
}

func (s *SelectQueryExprDsl) LeftJoin(table *Table) *JoinSpecStart {
	return s.join(table, LeftJoin)
}

func (s *SelectQueryExprDsl) LeftJoinQ(builder Builder[*SubQuery]) *JoinSpecStart {
	return s.join(builder.Build(), LeftJoin)
}

func (s *SelectQueryExprDsl) RightJoin(table *Table) *JoinSpecStart {
	return s.join(table, RightJoin)
}

func (s *SelectQueryExprDsl) RightJoinQ(builder Builder[*SubQuery]) *JoinSpecStart {
	return s.join(builder.Build(), RightJoin)
}

func (s *SelectQueryExprDsl) FullJoin(table *Table) *JoinSpecStart {
	return s.join(table, FullJoin)
}

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
	return qe
}

func (s *SelectQueryExprDsl) Build() *Selection {
	return s.selectDsl.Build()
}

func (s *SelectQueryExprDsl) Action(ctx context.Context, db *DB) error {
	return s.selectDsl.Action(ctx, db)
}

func (s *SelectQueryExprDsl) ActionTx(ctx context.Context, tx *TX) error {
	return s.selectDsl.ActionTx(ctx, tx)
}

type JoinSpecStart struct {
	joinTable    ITable
	joinType     JoinType
	queryExprDsl *SelectQueryExprDsl
}

func (j *JoinSpecStart) On(column string, condition JoinCondition, andCriteria ...*JoinCriterion) *JoinSpecEnd {
	return j.OnC(Col(column), condition, andCriteria...)
}

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

type JoinSpecEnd struct {
	joinSpec     *JoinSpec
	queryExprDsl *SelectQueryExprDsl
}

func (j *JoinSpecEnd) Build() *Selection {
	return j.queryExprDsl.Build()
}

func (j *JoinSpecEnd) Wheres() *QueryExprWhereBuilder {
	return j.queryExprDsl.Wheres()
}

func (j *JoinSpecEnd) And(column IColumn, condition JoinCondition) *JoinSpecEnd {
	joinCriterion := &JoinCriterion{
		connector:  "and",
		leftColumn: column,
		condition:  condition,
	}
	j.joinSpec.joinCriteria = append(j.joinSpec.joinCriteria, joinCriterion)
	return j
}

func (j *JoinSpecEnd) Join(table *Table) *JoinSpecStart {
	return j.queryExprDsl.Join(table)
}

func (j *JoinSpecEnd) JoinQ(builder Builder[*SubQuery]) *JoinSpecStart {
	return j.queryExprDsl.JoinQ(builder)
}

func Select(columns ...string) *FromGather {
	fg := new(FromGather)
	for _, column := range columns {
		fg.selectList = append(fg.selectList, Col(column))
	}
	fg.selectDsl = new(SelectDsl)
	return fg
}

func SelectDistinct(columns ...string) *FromGather {
	fg := Select(columns...)
	fg.distinct = true
	return fg
}

func SelectC(columns ...IColumn) *FromGather {
	fg := new(FromGather)
	for _, column := range columns {
		fg.selectList = append(fg.selectList, column)
	}
	fg.selectDsl = new(SelectDsl)
	return fg
}

func SelectDistinctC(columns ...IColumn) *FromGather {
	fg := SelectC(columns...)
	fg.distinct = true
	return fg
}

type SelectDsl struct {
	queryExprs []*SelectQueryExprDsl
	orderBy    *OrderByMdl
	limit      int
	offset     int

	target any
}

func (s *SelectDsl) Limit(limit int) *LimitEnd {
	s.limit = limit
	return &LimitEnd{
		selectDsl: s,
	}
}

func (s *SelectDsl) Offset(offset int) *OffsetEnd {
	s.offset = offset
	return &OffsetEnd{
		selectDsl: s,
	}
}

func (s *SelectDsl) OrderBy(spec ...OrderBySpec) {
	s.orderBy = &OrderByMdl{
		columns: spec,
	}
}

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

func (s *SelectDsl) As(alias string) *SubQueryDsl {
	return &SubQueryDsl{
		selectDsl: s,
		alias:     alias,
	}
}

func (s *SelectDsl) Into(target any) *SelectDsl {
	s.target = target
	return s
}

func (s *SelectDsl) Action(ctx context.Context, db *DB) error {
	renderCtx := &RenderCtx{
		dbType: db.dbType,
		cnt:    new(fn.Counter),
	}
	stm, args := s.Build().GetStatement(renderCtx).prepare()
	rows, err := db.QueryContext(ctx, stm, args...)
	defer func() {
		err = rows.Close()
		if err != nil {
			panic(err)
		}
	}()
	if err != nil {
		return err
	}
	return s.readRows(rows)
}

func (s *SelectDsl) ActionTx(ctx context.Context, tx *TX) error {
	renderCtx := &RenderCtx{
		dbType: tx.dbType,
		cnt:    new(fn.Counter),
	}
	stm, args := s.Build().GetStatement(renderCtx).prepare()
	rows, err := tx.QueryContext(ctx, stm, args...)
	defer func() {
		err = rows.Close()
		if err != nil {
			panic(err)
		}
	}()
	if err != nil {
		return err
	}
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
	for rows.Next() {
		var fields []any
		doVal := reflect.ValueOf(s.target)
		if doVal.Kind() != reflect.Pointer {
			return lei.New("The target is supposed to be of pointer kind")
		}
		doVal = doVal.Elem()
		for doVal.Kind() == reflect.Pointer {
			doVal = doVal.Elem()
		}
		for _, column := range columns {
			for i := 0; i < doVal.NumField(); i++ {
				if col, ok := doVal.Type().Field(i).Tag.Lookup("db"); ok && (col == column || strings.Split(col, ";")[0] == column) {
					fp := doVal.Field(i).Addr().Interface()
					fields = append(fields, fp)
					break
				}

			}
		}
		err = rows.Scan(fields...)
		if err != nil {
			return err
		}
	}
	return nil
}

type SubQueryDsl struct {
	selectDsl *SelectDsl
	alias     string
}

func (s *SubQueryDsl) Build() *SubQuery {
	return &SubQuery{
		selection: s.selectDsl.Build(),
		alias:     s.alias,
	}
}

type FromGather struct {
	connector  string
	selectList []IColumn
	distinct   bool
	selectDsl  *SelectDsl
}

func (f *FromGather) From(table string) *SelectQueryExprDsl {
	return f.FromT(Tbl(table))
}

func (f *FromGather) FromT(table ITable) *SelectQueryExprDsl {
	for _, col := range f.selectList {
		col.setNamespace(table.Alias().OrElse(table.Name()))
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

func (f *FromGather) FromQ(sb Builder[*SubQuery]) *SelectQueryExprDsl {
	table := sb.Build()
	return f.FromT(table)
}

func (f *FromGather) FromDO(do DO) *SelectQueryExprDsl {
	f.selectDsl.Into(do)
	return f.From(do.TableName())
}

type QueryExprWhereBuilder struct {
	firstCriterion Criterion
	subCriteria    []*AndOrCriteria
	queryExprDsl   *SelectQueryExprDsl
}

func (q *QueryExprWhereBuilder) Build() *Selection {
	return q.queryExprDsl.Build()
}

func (q *QueryExprWhereBuilder) Action(ctx context.Context, db *DB) error {
	return q.queryExprDsl.Action(ctx, db)
}

func (q *QueryExprWhereBuilder) ActionTx(ctx context.Context, tx *TX) error {
	return q.queryExprDsl.ActionTx(ctx, tx)
}

func (q *QueryExprWhereBuilder) BuildModel() *WhereMdl {
	return &WhereMdl{
		criterion:   q.firstCriterion,
		subCriteria: q.subCriteria,
	}
}

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

func (q *QueryExprWhereBuilder) AndC(criterion Criterion, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return q
}

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

func (q *QueryExprWhereBuilder) OrC(criterion Criterion, subCriteria ...*AndOrCriteria) *QueryExprWhereBuilder {
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return q
}

func (q *QueryExprWhereBuilder) Union() *UnionBuilder {
	return &UnionBuilder{
		queryExprDsl: q.queryExprDsl,
		connector:    "union",
	}
}

func (q *QueryExprWhereBuilder) UnionAll() *UnionBuilder {
	return &UnionBuilder{
		queryExprDsl: q.queryExprDsl,
		connector:    "union all",
	}
}

func (q *QueryExprWhereBuilder) OrderBy(columns ...OrderBySpec) *SelectDsl {
	q.queryExprDsl.selectDsl.orderBy.columns = columns
	return q.queryExprDsl.selectDsl
}

func (q *QueryExprWhereBuilder) GroupBy(columns ...string) *GroupByEnd {
	return q.queryExprDsl.GroupBy(columns...)
}

func (q *QueryExprWhereBuilder) GroupByC(columns ...IColumn) *GroupByEnd {
	return q.queryExprDsl.GroupByC(columns...)
}

func (q *QueryExprWhereBuilder) Limit(limit int) *LimitEnd {
	return q.queryExprDsl.Limit(limit)
}

func (q *QueryExprWhereBuilder) Offset(offset int) *OffsetEnd {
	return q.queryExprDsl.Offset(offset)
}

type QueryExprHavingBuilder struct {
	queryExprDsl   *SelectQueryExprDsl
	firstCriterion Criterion
	subCriteria    []*AndOrCriteria
}

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

func (q *QueryExprHavingBuilder) AndC(criterion Criterion, subCriteria ...*AndOrCriteria) *QueryExprHavingBuilder {
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "and",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return q
}

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

func (q *QueryExprHavingBuilder) OrC(criterion Criterion, subCriteria ...*AndOrCriteria) *QueryExprHavingBuilder {
	q.subCriteria = append(q.subCriteria, &AndOrCriteria{
		connector:      "or",
		firstCriterion: criterion,
		subCriteria:    subCriteria,
	})
	return q
}

func (q *QueryExprHavingBuilder) Offset(offset int) *OffsetEnd {
	return q.queryExprDsl.Offset(offset)
}

func (q *QueryExprHavingBuilder) Limit(limit int) *LimitEnd {
	return q.queryExprDsl.Limit(limit)
}

func (q *QueryExprHavingBuilder) OrderBy(spec ...OrderBySpec) *SelectDsl {
	return q.queryExprDsl.OrderBy(spec...)
}

func (q *QueryExprHavingBuilder) Union() *UnionBuilder {
	return q.queryExprDsl.Union()
}

func (q *QueryExprHavingBuilder) UnionAll() *UnionBuilder {
	return q.queryExprDsl.UnionAll()
}

func (q *QueryExprHavingBuilder) Build() *Selection {
	return q.queryExprDsl.Build()
}

type UnionBuilder struct {
	queryExprDsl *SelectQueryExprDsl
	connector    string
}

func (u *UnionBuilder) Select(columns ...IColumn) *FromGather {
	res := &FromGather{
		connector: u.connector,
		selectDsl: u.queryExprDsl.selectDsl,
		distinct:  false,
	}
	res.selectList = append(res.selectList, columns...)
	return res
}

func (u *UnionBuilder) SelectDistinct(columns ...IColumn) *FromGather {
	res := &FromGather{
		connector: u.connector,
		selectDsl: u.queryExprDsl.selectDsl,
		distinct:  true,
	}
	res.selectList = append(res.selectList, columns...)
	return res
}

type GroupByEnd struct {
	queryExprDsl *SelectQueryExprDsl
}

func (g *GroupByEnd) Action(ctx context.Context, db *DB) error {
	return g.queryExprDsl.Action(ctx, db)
}

func (g *GroupByEnd) ActionTx(ctx context.Context, tx *TX) error {
	return g.queryExprDsl.ActionTx(ctx, tx)
}

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

func (g *GroupByEnd) Havings() *QueryExprHavingBuilder {
	return g.queryExprDsl.Havings()
}

func (g *GroupByEnd) OrderBy(spec ...OrderBySpec) *SelectDsl {
	return g.queryExprDsl.OrderBy(spec...)
}

func (g *GroupByEnd) As(alias string) *SubQueryDsl {
	return g.queryExprDsl.selectDsl.As(alias)
}

func (g *GroupByEnd) Build() *Selection {
	return g.queryExprDsl.selectDsl.Build()
}

func (g *GroupByEnd) Union() *UnionBuilder {
	return g.queryExprDsl.Union()
}

func (g *GroupByEnd) UnionAll() *UnionBuilder {
	return g.queryExprDsl.UnionAll()
}

func (g *GroupByEnd) Limit(limit int) *LimitEnd {
	return g.queryExprDsl.Limit(limit)
}

func (g *GroupByEnd) Offset(offset int) *OffsetEnd {
	return g.queryExprDsl.Offset(offset)
}

type LimitEnd struct {
	selectDsl *SelectDsl
}

func (l *LimitEnd) Action(ctx context.Context, db *DB) error {
	return l.selectDsl.Action(ctx, db)
}

func (l *LimitEnd) ActionTx(ctx context.Context, tx *TX) error {
	return l.selectDsl.ActionTx(ctx, tx)
}

func (l *LimitEnd) Offset(offset int) *OffsetEnd {
	return l.selectDsl.Offset(offset)
}

func (l *LimitEnd) Build() *Selection {
	return l.selectDsl.Build()
}

func (l *LimitEnd) As(alias string) *SubQueryDsl {
	return l.selectDsl.As(alias)
}

type OffsetEnd struct {
	selectDsl *SelectDsl
}

func (o *OffsetEnd) Action(ctx context.Context, db *DB) error {
	return o.selectDsl.Action(ctx, db)
}

func (o *OffsetEnd) ActionTx(ctx context.Context, tx *TX) error {
	return o.selectDsl.ActionTx(ctx, tx)
}

func (o *OffsetEnd) Build() *Selection {
	return o.selectDsl.Build()
}

func (o *OffsetEnd) As(alias string) *SubQueryDsl {
	return o.selectDsl.As(alias)
}
