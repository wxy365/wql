package q

import (
	"testing"
)

// ── Basic SELECT ────────────────────────────────────────────────────────────

func TestSelectStar(t *testing.T) {
	stm := Select().From("tb_example").
		Where(Col("name"), Eq("wxy")).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `tb_example` where `name` = :1" {
		t.Errorf("got: %s", stm.stm)
	}
	if stm.params[1] != "wxy" {
		t.Errorf("param 1 = %v, want wxy", stm.params[1])
	}
}

func TestSelectColumns(t *testing.T) {
	stm := Select("id", "name", "age").From("user").
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select `user`.`id`, `user`.`name`, `user`.`age` from `user`" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestSelectDistinct(t *testing.T) {
	stm := SelectDistinct("name").From("user").
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select distinct `user`.`name` from `user`" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestSelectWithAlias(t *testing.T) {
	dsl := Select("u.id", "u.name").FromT(Tbl("user").As("u")).
		Where(Col("u.id"), Gt(5))
	stm := dsl.Build().GetStatement(getMysqlRenderCtx())
	want := "select `u`.`id`, `u`.`name` from `user` `u` where `u`.`id` > :1"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
	if stm.params[1] != 5 {
		t.Errorf("param 1 = %v, want 5", stm.params[1])
	}
}

// ── All Condition Types ─────────────────────────────────────────────────────

func TestConditionEq(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), Eq(1)).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` = :1" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionNe(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), Ne(1)).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` != :1" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionGt(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), Gt(10)).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` > :1" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionGe(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), Ge(10)).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` >= :1" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionLt(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), Lt(10)).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` < :1" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionLe(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), Le(10)).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` <= :1" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionLike(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), Like("%foo%")).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` like :1" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionNotLike(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), NotLike("%foo%")).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` not like :1" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionIn(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), In(1, 2, 3)).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` in (:1, :2, :3)" {
		t.Errorf("got: %s", stm.stm)
	}
	if stm.params[1] != 1 || stm.params[2] != 2 || stm.params[3] != 3 {
		t.Errorf("params mismatch: %v", stm.params)
	}
}

func TestConditionNotIn(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), NotIn(1, 2)).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` not in (:1, :2)" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionInSingle(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), In(42)).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` in (:1)" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionInEmpty(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), In()).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` in ()" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionIsNull(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), IsNull()).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` is null" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionIsNotNull(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), IsNotNull()).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` is not null" {
		t.Errorf("got: %s", stm.stm)
	}
}

func TestConditionBetween(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), Between(10, 20)).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` between :1 and :2" {
		t.Errorf("got: %s", stm.stm)
	}
	if stm.params[1] != 10 || stm.params[2] != 20 {
		t.Errorf("params mismatch: %v", stm.params)
	}
}

func TestConditionNotBetween(t *testing.T) {
	stm := Select().From("t").Where(Col("c"), NotBetween(1, 9)).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `c` not between :1 and :2" {
		t.Errorf("got: %s", stm.stm)
	}
}

// ── Column Comparison ──────────────────────────────────────────────────────

func TestColumnCompare(t *testing.T) {
	stm := Select().From("t").Where(Col("a"), ColumnCompare(Col("b"), "=")).
		Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "select * from `t` where `a` = `b`" {
		t.Errorf("got: %s", stm.stm)
	}
}

// ── AND / OR Chaining ──────────────────────────────────────────────────────

func TestWhereAnd(t *testing.T) {
	stm := Select().From("t").
		Where(Col("a"), Gt(10)).
		And(Col("b"), Lt(20)).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `t` where `a` > :1 and (`b` < :2)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestWhereOr(t *testing.T) {
	stm := Select().From("t").
		Where(Col("a"), Eq(1)).
		Or(Col("b"), Eq(2)).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `t` where `a` = :1 or (`b` = :2)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestWhereAndOrGrouped(t *testing.T) {
	stm := Select().From("t").
		Where(Col("a"), Eq(1)).
		And(Col("b"), Eq(2),
			&AndOrCriteria{
				connector: "or",
				firstCriterion: &ColumnConditionCriterion{
					column:    Col("c"),
					condition: Eq(3),
				},
				subCriteria: []*AndOrCriteria{
					{
						connector: "or",
						firstCriterion: &ColumnConditionCriterion{
							column:    Col("c"),
							condition: Eq(4),
						},
					},
				},
			},
		).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `t` where `a` = :1 and (`b` = :2 or (`c` = :3 or (`c` = :4)))"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── ORDER BY ────────────────────────────────────────────────────────────────

func TestOrderBy(t *testing.T) {
	stm := Select().From("t").
		Where(Col("a"), Gt(0)).
		OrderBy(Col("a"), Col("b")).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `t` where `a` > :1 order by `a`, `b`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestOrderByDesc(t *testing.T) {
	stm := Select().From("t").
		OrderBy(Col("a").Desc()).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `t` order by `a` desc"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── GROUP BY / HAVING ───────────────────────────────────────────────────────

func TestGroupBy(t *testing.T) {
	stm := SelectC(Col("dept"), Sum("salary").As("total")).
		From("emp").
		GroupBy("dept").
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `emp`.`dept`, sum(`salary`) `total` from `emp` group by `dept`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestGroupByHaving(t *testing.T) {
	stm := SelectC(Col("dept"), Sum("salary").As("total")).
		From("emp").
		GroupBy("dept").
		Having(Col("total"), Gt(10000)).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `emp`.`dept`, sum(`salary`) `total` from `emp` group by `dept` having `total` > :1"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestGroupByHavingAnd(t *testing.T) {
	stm := SelectC(Col("dept"), Sum("salary").As("total"), Max("salary").As("max_sal")).
		From("emp").
		GroupBy("dept").
		Having(Col("total"), Gt(10000)).
		And(Col("max_sal"), Lt(50000)).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `emp`.`dept`, sum(`salary`) `total`, max(`salary`) `max_sal` from `emp` group by `dept` having `total` > :1 and (`max_sal` < :2)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── LIMIT / OFFSET ──────────────────────────────────────────────────────────

func TestLimit(t *testing.T) {
	stm := Select().From("t").Limit(10).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `t` limit 10"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestLimitOffset(t *testing.T) {
	stm := Select().From("t").Limit(10).Offset(20).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `t` limit 20, 10"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── JOIN ────────────────────────────────────────────────────────────────────

func TestJoin(t *testing.T) {
	stm := Select("e.id", "d.name").
		FromT(Tbl("emp").As("e")).
		Join(Tbl("dept d")).
		On("e.dept_id", EqualTo("d.id")).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `e`.`id`, `d`.`name` from `emp` `e` join `dept` `d` on `e`.`dept_id` = `d`.`id`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestJoinMultiple(t *testing.T) {
	stm := Select("e.id", "d.name", "l.city").
		FromT(Tbl("emp").As("e")).
		Join(Tbl("dept d")).
		On("e.dept_id", EqualTo("d.id")).
		Join(Tbl("loc l")).
		On("d.loc_id", EqualTo("l.id")).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `e`.`id`, `d`.`name`, `l`.`city` from `emp` `e` join `dept` `d` on `e`.`dept_id` = `d`.`id` join `loc` `l` on `d`.`loc_id` = `l`.`id`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestLeftJoin(t *testing.T) {
	stm := Select("e.id", "d.name").
		FromT(Tbl("emp").As("e")).
		LeftJoin(Tbl("dept d")).
		On("e.dept_id", EqualTo("d.id")).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `e`.`id`, `d`.`name` from `emp` `e` left join `dept` `d` on `e`.`dept_id` = `d`.`id`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestFullJoin(t *testing.T) {
	stm := Select("e.id", "d.name").
		FromT(Tbl("emp").As("e")).
		FullJoin(Tbl("dept d")).
		On("e.dept_id", EqualTo("d.id")).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `e`.`id`, `d`.`name` from `emp` `e` full join `dept` `d` on `e`.`dept_id` = `d`.`id`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── UNION / UNION ALL ────────────────────────────────────────────────────────

func TestUnion(t *testing.T) {
	stm := Select("name").From("users").
		Union().
		Select(Col("name")).
		From("guests").
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `users`.`name` from `users` union (select `guests`.`name` from `guests`)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestUnionAll(t *testing.T) {
	stm := Select("name").From("users").
		UnionAll().
		Select(Col("name")).
		From("guests").
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `users`.`name` from `users` union all (select `guests`.`name` from `guests`)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ──────────────────────── SUBQUERY TESTS ─────────────────────────────────────

// ── SubQueryColumn in SELECT list ───────────────────────────────────────────

func TestSubQueryColumnInSelect(t *testing.T) {
	stm := SelectC(
		Col("id"), Col("name"),
		SubQ(SelectC(Sum("amount")).From("orders").Where(Col("user_id"), ColumnCompare(Col("id"), "="))).As("total"),
	).FromT(Tbl("users").As("u")).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `u`.`id`, `u`.`name`, (select sum(`amount`) from `orders` where `user_id` = `id`) `total` from `users` `u`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestSubQueryColumnWithoutAlias(t *testing.T) {
	stm := SelectC(
		Col("id"),
		SubQ(SelectC(Col("amount")).From("orders").Where(Col("user_id"), ColumnCompare(Col("id"), "="))),
	).From("users").
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `users`.`id`, (select `orders`.`amount` from `orders` where `user_id` = `id`) from `users`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── Subquery in WHERE: scalar comparisons ───────────────────────────────────

func TestSubQueryInWhereEq(t *testing.T) {
	stm := Select().From("emp").
		Where(Col("salary"), EqSubQ(SelectC(Max("salary")).From("emp"))).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `emp` where `salary` = (select max(`salary`) from `emp`)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestSubQueryInWhereNe(t *testing.T) {
	stm := Select().From("emp").
		Where(Col("dept"), NeSubQ(Select("name").From("dept").Where(Col("id"), Eq(1)))).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `emp` where `dept` != (select `dept`.`name` from `dept` where `id` = :1)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestSubQueryInWhereGt(t *testing.T) {
	stm := Select().From("emp").
		Where(Col("salary"), GtSubQ(SelectC(Avg("salary")).From("emp"))).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `emp` where `salary` > (select avg(`salary`) from `emp`)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestSubQueryInWhereGe(t *testing.T) {
	stm := Select().From("emp").
		Where(Col("salary"), GeSubQ(SelectC(Max("salary")).From("emp"))).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `emp` where `salary` >= (select max(`salary`) from `emp`)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestSubQueryInWhereLt(t *testing.T) {
	stm := Select().From("emp").
		Where(Col("salary"), LtSubQ(SelectC(Avg("salary")).From("emp"))).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `emp` where `salary` < (select avg(`salary`) from `emp`)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestSubQueryInWhereLe(t *testing.T) {
	stm := Select().From("emp").
		Where(Col("salary"), LeSubQ(SelectC(Min("salary")).From("emp"))).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `emp` where `salary` <= (select min(`salary`) from `emp`)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestSubQueryInWhereIn(t *testing.T) {
	stm := Select().From("emp").
		Where(Col("dept_id"), InSubQ(Select("id").From("dept").Where(Col("active"), Eq(true)))).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `emp` where `dept_id` in (select `dept`.`id` from `dept` where `active` = :1)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
	if stm.params[1] != true {
		t.Errorf("param 1 = %v, want true", stm.params[1])
	}
}

func TestSubQueryInWhereNotIn(t *testing.T) {
	stm := Select().From("emp").
		Where(Col("dept_id"), NotInSubQ(Select("id").From("dept"))).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `emp` where `dept_id` not in (select `dept`.`id` from `dept`)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── SubQuery in FROM ─────────────────────────────────────────────────────────

func TestSubQueryInFrom(t *testing.T) {
	stm := Select("sq.dept", "sq.total").
		FromQ(
			SelectC(Col("dept"), Sum("salary").As("total")).
				From("emp").
				GroupBy("dept").
				As("sq"),
		).
		Where(Col("sq.total"), Gt(1)).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `sq`.`dept`, `sq`.`total` from (select `emp`.`dept`, sum(`salary`) `total` from `emp` group by `dept`) sq where `sq`.`total` > :1"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestSubQueryInFromSimple(t *testing.T) {
	stm := Select("x.max_sal").
		FromQ(
			SelectC(Max("salary").As("max_sal")).From("emp").As("x"),
		).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `x`.`max_sal` from (select max(`salary`) `max_sal` from `emp`) x"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── SubQuery in JOIN ────────────────────────────────────────────────────────

func TestSubQueryJoin(t *testing.T) {
	dsl := Select("e.employee_id", "e.first_name", "e.last_name", "e.department",
		"total_sales.total_amount").
		FromT(Tbl("employees").As("e")).
		JoinQ(
			SelectC(Col("employee_id"), Sum("sale_amount").As("total_amount")).
				From("sales").
				GroupBy("employee_id").
				OrderBy(Col("total_amount").Desc()).
				Limit(1).
				As("total_sales"),
		).
		On("e.employee_id", EqualTo("total_sales.employee_id"))
	stm := dsl.Build().GetStatement(getMysqlRenderCtx())
	want := "select `e`.`employee_id`, `e`.`first_name`, `e`.`last_name`, `e`.`department`, `total_sales`.`total_amount` from `employees` `e` join (select `sales`.`employee_id`, sum(`sale_amount`) `total_amount` from `sales` group by `employee_id` order by `total_amount` desc limit 1) total_sales on `e`.`employee_id` = `total_sales`.`employee_id`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestLeftJoinSubQuery(t *testing.T) {
	stm := Select("e.id", "d.name").
		FromT(Tbl("emp").As("e")).
		LeftJoinQ(
			Select("id", "name").From("dept").As("d"),
		).
		On("e.dept_id", EqualTo("d.id")).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `e`.`id`, `d`.`name` from `emp` `e` left join (select `dept`.`id`, `dept`.`name` from `dept`) d on `e`.`dept_id` = `d`.`id`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── EXISTS / NOT EXISTS ─────────────────────────────────────────────────────

func TestExists(t *testing.T) {
	stm := Select().From("emp").
		WhereE(
			Exists(Select().From("dept").
				Where(Col("dept.id"), ColumnCompare(Col("emp.dept_id"), "="))),
		).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `emp` where exists (select * from `dept` where `dept`.`id` = `emp`.`dept_id`)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestNotExists(t *testing.T) {
	stm := Select().From("emp").
		WhereE(
			NotExists(Select().From("dept").
				Where(Col("dept.id"), ColumnCompare(Col("emp.dept_id"), "="))),
		).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `emp` where not exists (select * from `dept` where `dept`.`id` = `emp`.`dept_id`)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── Nested Subqueries ───────────────────────────────────────────────────────

func TestNestedSubQuery(t *testing.T) {
	// SELECT * FROM t WHERE a > (SELECT avg(x) FROM (SELECT * FROM s) AS sq)
	inner := Select().From("s").As("sq")
	stm := Select().From("t").
		Where(Col("a"), GtSubQ(SelectC(Avg("x")).FromQ(inner))).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `t` where `a` > (select avg(`x`) from (select * from `s`) sq)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── INSERT ──────────────────────────────────────────────────────────────────

func TestInsertDO(t *testing.T) {
	u := &User{
		Name:    "wxy",
		Age:     18,
		Address: "xxx city",
	}
	stm := InsertDO(u).Build().GetStatement(getMysqlRenderCtx())
	if stm.stm != "insert into user(name, age, addr) values (?, ?, ?)" {
		t.Errorf("got: %s", stm.stm)
	}
	if stm.params[1] != "wxy" || stm.params[2] != 18 || stm.params[3] != "xxx city" {
		t.Errorf("params mismatch: %v", stm.params)
	}
}

// ── UPDATE ──────────────────────────────────────────────────────────────────

func TestUpdateSetWhere(t *testing.T) {
	stm := Update("user").
		Set("name").EqualTo("new name").
		Where(Col("id"), Eq(1)).
		Build().GetStatement(getMysqlRenderCtx())
	want := "update user set `name` = ? where `id` = :2"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
	if stm.params[2] != 1 {
		t.Errorf("param 2 = %v, want 1", stm.params[2])
	}
}

func TestUpdateSetMultiple(t *testing.T) {
	stm := Update("user").
		Set("name").EqualTo("foo").
		Set("age").EqualTo(30).
		Where(Col("id"), Eq(1)).
		Build().GetStatement(getMysqlRenderCtx())
	want := "update user set `name` = ?, `age` = ? where `id` = :3"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
	if stm.params[3] != 1 {
		t.Errorf("param 3 = %v, want 1", stm.params[3])
	}
}

func TestUpdateOrderByLimit(t *testing.T) {
	stm := Update("user").
		Set("status").EqualTo("inactive").
		Where(Col("last_login"), Lt("2020-01-01")).
		OrderBy(Col("last_login")).
		Limit(100).
		Build().GetStatement(getMysqlRenderCtx())
	want := "update user set `status` = ? where `last_login` < :2 order by `last_login` limit 100"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── DELETE ──────────────────────────────────────────────────────────────────

func TestDelete(t *testing.T) {
	stm := Delete().From("user").Where(Col("status"), Eq("inactive")).
		Build().GetStatement(getMysqlRenderCtx())
	want := "delete from `user` where `status` = :1"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestDeleteOrderByLimit(t *testing.T) {
	stm := Delete().From("user").
		Where(Col("status"), Eq("inactive")).
		OrderBy(Col("created_at")).
		Limit(10).
		Build().GetStatement(getMysqlRenderCtx())
	want := "delete from `user` where `status` = :1 order by `created_at` limit 10"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── COUNT ───────────────────────────────────────────────────────────────────

func TestCountStar(t *testing.T) {
	stm := Count("*").From("user").Build().GetStatement(getMysqlRenderCtx())
	want := "select count(`user`.*) from `user`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestCountColumn(t *testing.T) {
	stm := Count("name").From("user").
		Where(Col("status"), Eq("active")).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select count(`user`.`name`) from `user` where `status` = :1"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestCountDistinct(t *testing.T) {
	stm := CountDistinct("name").From("user").
		Build().GetStatement(getMysqlRenderCtx())
	want := "select count(distinct `user`.`name`) from `user`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestCountWithAlias(t *testing.T) {
	stm := CountC(Col("id")).As("cnt").
		FromT(Tbl("user").As("u")).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select count(`u`.`id`) `cnt` from `user` `u`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── Column DSL parsing ─────────────────────────────────────────────────────

func TestColParsing(t *testing.T) {
	c := Col("u.name full_name")
	if c.Name() != "name" {
		t.Errorf("Name = %s, want name", c.Name())
	}
	ns := c.namespace().Get()
	if ns != "u" {
		t.Errorf("namespace = %s, want u", ns)
	}
	alias := c.Alias().Get()
	if alias != "full_name" {
		t.Errorf("alias = %s, want full_name", alias)
	}
}

func TestTableAlias(t *testing.T) {
	tbl := Tbl("user u")
	if tbl.Name() != "user" {
		t.Errorf("Name = %s, want user", tbl.Name())
	}
	alias := tbl.Alias().Get()
	if alias != "u" {
		t.Errorf("alias = %s, want u", alias)
	}
}

// ── Aggregate Functions ─────────────────────────────────────────────────────

func TestAvgAggregate(t *testing.T) {
	stm := SelectC(Col("dept"), Avg("salary").As("avg_sal")).
		From("emp").
		GroupBy("dept").
		Build().GetStatement(getMysqlRenderCtx())
	want := "select `emp`.`dept`, avg(`salary`) `avg_sal` from `emp` group by `dept`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

func TestMinMaxAggregate(t *testing.T) {
	stm := SelectC(Min("salary"), Max("salary")).From("emp").
		Build().GetStatement(getMysqlRenderCtx())
	want := "select min(`salary`), max(`salary`) from `emp`"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
}

// ── Scalar subquery with params ─────────────────────────────────────────────

func TestSubQueryWithParams(t *testing.T) {
	stm := Select().From("emp").
		Where(Col("salary"), GtSubQ(
			SelectC(Avg("salary")).From("emp").Where(Col("dept_id"), Eq(10)),
		)).
		Build().GetStatement(getMysqlRenderCtx())
	want := "select * from `emp` where `salary` > (select avg(`salary`) from `emp` where `dept_id` = :1)"
	if stm.stm != want {
		t.Errorf("got:\n%s\nwant:\n%s", stm.stm, want)
	}
	if stm.params[1] != 10 {
		t.Errorf("param 1 = %v, want 10", stm.params[1])
	}
}
