package q

import (
	"github.com/wxy365/basal/fn"
	"testing"
)

func TestSelect(t *testing.T) {
	dsl := Select().From("tb_example").
		Where(Col("name"), Eq("wxy"))
	selection := dsl.Build()
	statement := selection.GetStatement(getMysqlRenderCtx())
	if statement.stm != "select * from `tb_example` where `name` = :1" {
		t.Fail()
	}
	if p, ok := statement.params[1]; ok {
		if p != "wxy" {
			t.Fail()
		}
	} else {
		t.Fail()
	}
}

func TestSelect1(t *testing.T) {
	/*
			`
		SELECT
		    e.employee_id,
		    e.first_name,
		    e.last_name,
		    e.department,
		    e.hire_date,
		    total_sales.total_amount
		FROM
		    employees e
		JOIN
		    (SELECT
		         employee_id,
		         SUM(sale_amount) AS total_amount
		     FROM
		         sales
		     GROUP BY
		         employee_id
		     ORDER BY
		         total_amount DESC
		     LIMIT 1) AS total_sales
		ON
		    e.employee_id = total_sales.employee_id;
		`
	*/

	dsl := Select("e.employee_id", "e.first_name", "e.last_name", "e.department", "e.hire_date", "total_sales.total_amount").
		FromT(Tbl("employees").As("e")).
		JoinQ(SelectC(Col("employee_id"), Sum("sale_amount").As("total_amount")).
			From("sales").
			GroupBy("employee_id").
			OrderBy(Col("total_amount").Desc()).
			Limit(1).
			As("total_sales")).
		On("e.employee_id", EqualTo("total_sales.employee_id"))
	selection := dsl.Build()
	statement := selection.GetStatement(getMysqlRenderCtx())
	if statement.stm != "select `e`.`employee_id`, `e`.`first_name`, `e`.`last_name`, `e`.`department`, `e`.`hire_date`, `e`.`total_amount` from `employees` `e` join (select `sales`.`employee_id`, sum(`sale_amount`) `total_amount` from `sales` group by `employee_id` order by `total_amount` desc limit 1) total_sales on `e`.`employee_id` = `total_sales`.`employee_id`" {
		t.Fail()
	}
}

func getMysqlRenderCtx() *RenderCtx {
	cnt := new(fn.Counter)
	return &RenderCtx{
		dbType: MySQL,
		cnt:    cnt,
	}
}
