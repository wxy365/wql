package q

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/wxy365/basal/errs"
	"github.com/wxy365/basal/text"
)

type Insertion[T any] struct {
	table *Table
	rows  []T
}

func (i *Insertion[T]) GetStatement(ctx *RenderCtx) *Statement {

	if len(i.rows) == 0 {
		panic(errs.New("Empty payload to insert"))
	}
	escaper := ctx.dbType.escaper()
	var fields []string
	var fieldIndexes []int
	t := reflect.TypeOf(i.rows[0])
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	for j := 0; j < t.NumField(); j++ {
		f := t.Field(j)
		if col, ok := f.Tag.Lookup("db"); ok && col != "" && !strings.HasSuffix(col, ";auto_incr") {
			fields = append(fields, escaper(strings.Split(col, ";")[0]))
			fieldIndexes = append(fieldIndexes, j)
		}
	}
	if len(fields) == 0 {
		panic(errs.New("The struct [{0}] has no field mapped to table column", t.Name()))
	}
	b := text.Build("insert into ", escaper(i.table.name), "(", strings.Join(fields, ", "), ") values ")

	params := make(map[int]any)
	var valueGroups []string
	for _, row := range i.rows {
		val := reflect.ValueOf(row)
		for val.Kind() == reflect.Pointer {
			val = val.Elem()
		}
		placeholders := make([]string, 0, len(fieldIndexes))
		for _, idx := range fieldIndexes {
			fv := val.Field(idx)
			placeholder := ctx.cnt.Incr()
			params[int(placeholder)] = fv.Interface()
			placeholders = append(placeholders, ":"+strconv.FormatInt(placeholder, 10))
		}
		valueGroups = append(valueGroups, "("+strings.Join(placeholders, ", ")+")")
	}
	return &Statement{
		stm:    b.String() + strings.Join(valueGroups, ","),
		params: params,
	}
}
