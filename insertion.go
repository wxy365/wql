package q

import (
	"reflect"
	"strings"

	"github.com/wxy365/basal/ds/slices"
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
	var fields []string
	var fieldIndexes []int
	t := reflect.TypeOf(i.rows[0])
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	for j := 0; j < t.NumField(); j++ {
		f := t.Field(j)
		if col, ok := f.Tag.Lookup("db"); ok && col != "" && !strings.HasSuffix(col, ";auto_incr") {
			fields = append(fields, col)
			fieldIndexes = append(fieldIndexes, j)
		}
	}
	if len(fields) == 0 {
		panic(errs.New("The struct [{0}] has no field mapped to table column", t.Name()))
	}
	b := text.Build("insert into ", i.table.name, "(", strings.Join(fields, ", "), ") values ")

	values := "(" + strings.Join(slices.New[[]string]("?", len(fields)), ", ") + ")"

	params := make(map[int]any)
	for _, t := range i.rows {
		b.Push(values, ",")
		val := reflect.ValueOf(t)
		for val.Kind() == reflect.Pointer {
			val = val.Elem()
		}
		for _, idx := range fieldIndexes {
			fv := val.Field(idx)
			placeholder := ctx.cnt.Incr()
			params[int(placeholder)] = fv.Interface()
		}
	}
	stm := b.String()
	return &Statement{
		stm:    stm[:len(stm)-1],
		params: params,
	}
}
