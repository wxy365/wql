package q

import (
	"context"
	"testing"
)

// prepareOf renders a statement and returns the SQL and the bound values that
// are actually handed over to the database driver.
func prepareOf(t *testing.T, stm *Statement) (string, []any) {
	t.Helper()
	sql, args := stm.prepare()
	return sql, args
}

// ── Placeholder rendering ────────────────────────────────────────────────────

// A statement ending with a placeholder used to panic with an out of range
// slice, which made every single-parameter query fail.
func TestPreparePlaceholderAtEndOfStatement(t *testing.T) {
	sql, args := prepareOf(t, &Statement{
		stm:    "select * from `t` where `c` = :1",
		params: map[int]any{1: "wxy"},
	})
	if sql != "select * from `t` where `c` = ?" {
		t.Errorf("got: %s", sql)
	}
	if len(args) != 1 || args[0] != "wxy" {
		t.Errorf("args = %v, want [wxy]", args)
	}
}

// The character following a placeholder used to be swallowed, which glued the
// placeholder to the next token of the statement.
func TestPrepareKeepsCharacterAfterPlaceholder(t *testing.T) {
	sql, args := prepareOf(t, &Statement{
		stm:    "select * from `t` where `a` = :1 and (`b` = :2)",
		params: map[int]any{1: 1, 2: 2},
	})
	if sql != "select * from `t` where `a` = ? and (`b` = ?)" {
		t.Errorf("got: %s", sql)
	}
	if len(args) != 2 || args[0] != 1 || args[1] != 2 {
		t.Errorf("args = %v, want [1 2]", args)
	}
}

// The bound values must be handed over in the order the placeholders appear.
func TestPrepareKeepsPlaceholderOrder(t *testing.T) {
	sql, args := prepareOf(t, &Statement{
		stm:    "select * from `t` where `a` = :2 and `b` = :1",
		params: map[int]any{1: "first", 2: "second"},
	})
	if sql != "select * from `t` where `a` = ? and `b` = ?" {
		t.Errorf("got: %s", sql)
	}
	if len(args) != 2 || args[0] != "second" || args[1] != "first" {
		t.Errorf("args = %v, want [second first]", args)
	}
}

// A colon that does not introduce a bound value is part of the literal text.
func TestPrepareKeepsNonPlaceholderColon(t *testing.T) {
	sql, args := prepareOf(t, &Statement{
		stm:    "select * from `t` where `c` = '12:30'",
		params: map[int]any{},
	})
	if sql != "select * from `t` where `c` = '12:30'" {
		t.Errorf("got: %s", sql)
	}
	if len(args) != 0 {
		t.Errorf("args = %v, want none", args)
	}
}

func TestPrepareOfNilStatement(t *testing.T) {
	var stm *Statement
	sql, args := prepareOf(t, stm)
	if sql != "" || len(args) != 0 {
		t.Errorf("got: %q, %v", sql, args)
	}
}

// ── INSERT / UPDATE / DELETE carry their bound values ────────────────────────

// The values of an insert used to be dropped: the statement held bare '?'
// placeholders that no bound value was ever matched against.
func TestInsertBindsItsValues(t *testing.T) {
	u := &User{Id: 1, Name: "wxy", Age: 18, Address: "xxx city"}
	sql, args := prepareOf(t, InsertDO(u).Build().GetStatement(getMysqlRenderCtx()))
	want := "insert into `user`(`name`, `age`, `addr`) values (?, ?, ?)"
	if sql != want {
		t.Errorf("got:\n%s\nwant:\n%s", sql, want)
	}
	if len(args) != 3 || args[0] != "wxy" || args[1] != 18 || args[2] != "xxx city" {
		t.Errorf("args = %v, want [wxy 18 xxx city]", args)
	}
}

func TestInsertMultipleRowsBindsItsValues(t *testing.T) {
	u1 := &User{Name: "wxy", Age: 18, Address: "xxx city"}
	u2 := &User{Name: "abc", Age: 20, Address: "yyy city"}
	sql, args := prepareOf(t, InsertDO(u1, u2).Build().GetStatement(getMysqlRenderCtx()))
	want := "insert into `user`(`name`, `age`, `addr`) values (?, ?, ?),(?, ?, ?)"
	if sql != want {
		t.Errorf("got:\n%s\nwant:\n%s", sql, want)
	}
	if len(args) != 6 || args[3] != "abc" || args[4] != 20 {
		t.Errorf("args = %v, want 6 values", args)
	}
}

// The value of the SET clause used to be dropped, only the WHERE clause kept
// its value.
func TestUpdateBindsItsValues(t *testing.T) {
	sql, args := prepareOf(t, Update("user").
		Set("name").EqualTo("new name").
		Where(Col("id"), Eq(1)).
		Build().GetStatement(getMysqlRenderCtx()))
	want := "update `user` set `name` = ? where `id` = ?"
	if sql != want {
		t.Errorf("got:\n%s\nwant:\n%s", sql, want)
	}
	if len(args) != 2 || args[0] != "new name" || args[1] != 1 {
		t.Errorf("args = %v, want [new name 1]", args)
	}
}

func TestDeleteBindsItsValues(t *testing.T) {
	sql, args := prepareOf(t, Delete().From("user").
		Where(Col("status"), Eq("inactive")).
		Build().GetStatement(getMysqlRenderCtx()))
	want := "delete from `user` where `status` = ?"
	if sql != want {
		t.Errorf("got:\n%s\nwant:\n%s", sql, want)
	}
	if len(args) != 1 || args[0] != "inactive" {
		t.Errorf("args = %v, want [inactive]", args)
	}
}

// ── COUNT ────────────────────────────────────────────────────────────────────

// "count(`user`.*)" is not valid SQL, the star must stay unqualified.
func TestCountStarIsNotQualified(t *testing.T) {
	stm := Count("*").From("user").Build().GetStatement(getMysqlRenderCtx())
	want := "select count(*) from `user`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestCountFromStarIsNotQualified(t *testing.T) {
	stm := CountFrom("user").Build().GetStatement(getMysqlRenderCtx())
	want := "select count(*) from `user`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestCountColumnIsQualified(t *testing.T) {
	stm := Count("name").From("user").Build().GetStatement(getMysqlRenderCtx())
	want := "select count(`user`.`name`) from `user`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── HAVING ───────────────────────────────────────────────────────────────────

// The sub-criteria of a HAVING clause used to be dropped altogether.
func TestHavingKeepsItsSubCriteria(t *testing.T) {
	stm := SelectC(Col("dept"), Sum("salary").As("total")).
		From("emp").
		GroupBy("dept").
		Having(Col("total"), Gt(10000),
			&AndOrCriteria{
				connector:      "or",
				firstCriterion: &ColumnConditionCriterion{column: Col("total"), condition: Lt(1)},
			}).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `emp`.`dept`, sum(`salary`) `total` from `emp` group by `dept` having `total` > :1 or (`total` < :2)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── LIMIT / OFFSET ───────────────────────────────────────────────────────────

// An offset without a limit used to be dropped, silently returning the first
// page instead of the requested one.
func TestOffsetWithoutLimit(t *testing.T) {
	stm := Select().From("t").Offset(20).Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `t` limit 18446744073709551615, 20"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestLimitWithOffset(t *testing.T) {
	stm := Select().From("t").Limit(10).Offset(20).Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `t` limit 20, 10"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── Column namespace ─────────────────────────────────────────────────────────

// A column reused by a second query used to keep the namespace of the first
// one, and was therefore qualified with the wrong table.
func TestColumnReusedAcrossTablesIsRequalified(t *testing.T) {
	shared := Col("id")
	first := SelectC(shared).From("table_a").Build().GetStatement(getMysqlRenderCtx())
	second := SelectC(shared).From("table_b").Build().GetStatement(getMysqlRenderCtx())
	if first.stm != "select `table_a`.`id` from `table_a`" {
		t.Errorf("got: %s", first.stm)
	}
	if second.stm != "select `table_b`.`id` from `table_b`" {
		t.Errorf("got: %s", second.stm)
	}
}

// A namespace given by the user is never overwritten by the table of the query.
func TestExplicitColumnNamespaceIsKept(t *testing.T) {
	stm := SelectC(Col("u.id")).FromT(Tbl("user").As("x")).Build().GetStatement(getMysqlRenderCtx())
	want := "select `u`.`id` from `user` `x`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── Empty clauses and missing aliases ────────────────────────────────────────

// A WHERE clause without any condition used to render a dangling "where ".
func TestEmptyWhereClauseIsLeftOut(t *testing.T) {
	stm := Select().From("t").Wheres().Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `t`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// A sub-query without an alias used to panic on an empty optional value.
func TestSubQueryWithoutAlias(t *testing.T) {
	sub := &SubQuery{selection: Select().From("t").Build()}
	stm := sub.GetStatement(getMysqlRenderCtx())
	want := "(select * from `t`)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── Execution without a data source ──────────────────────────────────────────

// A missing data source used to panic with a nil pointer dereference.
func TestActionWithoutDataSourceReportsAnError(t *testing.T) {
	saved := DataSource
	DataSource = nil
	defer func() { DataSource = saved }()

	if err := Select().From("t").Where(Col("id"), Eq(1)).Action(context.Background()); err == nil {
		t.Error("want an error when no data source is configured")
	}
	if _, err := Count("id").From("t").Action(context.Background()); err == nil {
		t.Error("want an error when no data source is configured")
	}
	if err := Update("t").Set("a").EqualTo(1).Action(context.Background()); err == nil {
		t.Error("want an error when no data source is configured")
	}
	if err := InsertDO(&User{Name: "wxy"}).Action(context.Background()); err == nil {
		t.Error("want an error when no data source is configured")
	}
	if err := Delete().From("t").Where(Col("id"), Eq(1)).Action(context.Background()); err == nil {
		t.Error("want an error when no data source is configured")
	}
}

func TestActionTxWithoutTransactionReportsAnError(t *testing.T) {
	if err := Select().From("t").ActionTx(context.Background(), nil); err == nil {
		t.Error("want an error when no transaction is given")
	}
	if err := Delete().From("t").ActionTx(context.Background(), nil); err == nil {
		t.Error("want an error when no transaction is given")
	}
	if err := Update("t").Set("a").EqualTo(1).ActionTx(context.Background(), nil); err == nil {
		t.Error("want an error when no transaction is given")
	}
}

// ── Auto increment write back ────────────────────────────────────────────────

// Only the last row of a multi-row insert used to receive its generated id.
func TestAutoIncrementIsWrittenBackToEveryRow(t *testing.T) {
	u1 := &User{Name: "wxy"}
	u2 := &User{Name: "abc"}
	writeBackAutoIncrId([]*User{u1, u2}, 10)
	if u1.Id != 10 || u2.Id != 11 {
		t.Errorf("ids = %d, %d, want 10, 11", u1.Id, u2.Id)
	}
}
