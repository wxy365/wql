package q

import (
	"context"
	"reflect"
	"strings"

	"github.com/wxy365/basal/errs"
	"github.com/wxy365/basal/fn"
)

type InsertDsl[T any] struct {
	rows  []T
	table *Table
}

func (i *InsertDsl[T]) Build() *Insertion[T] {
	return &Insertion[T]{
		rows:  i.rows,
		table: i.table,
	}
}

func (i *InsertDsl[T]) Action(ctx context.Context, optDb ...*DB) error {
	db, err := resolveDB(optDb...)
	if err != nil {
		return err
	}
	renderCtx := &RenderCtx{
		dbType: db.dbType,
		cnt:    new(fn.Counter),
	}
	stm, params := i.Build().GetStatement(renderCtx).prepare()
	result, err := db.ExecContext(ctx, stm, params...)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	writeBackAutoIncrId(i.rows, id)
	return nil
}

func (i *InsertDsl[T]) ActionTx(ctx context.Context, tx *TX) error {
	tx, err := resolveTx(tx)
	if err != nil {
		return err
	}
	renderCtx := &RenderCtx{
		dbType: tx.dbType,
		cnt:    new(fn.Counter),
	}
	stm, params := i.Build().GetStatement(renderCtx).prepare()
	result, err := tx.ExecContext(ctx, stm, params...)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	writeBackAutoIncrId(i.rows, id)
	return nil
}

// writeBackAutoIncrId writes the id generated for the first inserted row back
// into the auto-increment field of every inserted row. The ids of a multi-row
// insert are consecutive, so the id of the row at index i is firstId + i.
//
// Rows that are not addressable (a struct value rather than a pointer to one)
// are left untouched, since their id cannot be written back.
func writeBackAutoIncrId[T any](rows []T, firstId int64) {
	for idx, row := range rows {
		val := reflect.ValueOf(row)
		if val.Kind() == reflect.Pointer {
			val = val.Elem()
		}
		if val.Kind() != reflect.Struct || !val.CanAddr() {
			continue
		}
		for j := 0; j < val.NumField(); j++ {
			col, ok := val.Type().Field(j).Tag.Lookup("db")
			if !ok || !strings.HasSuffix(col, ";auto_incr") {
				continue
			}
			f := val.Field(j)
			switch f.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if f.CanSet() {
					f.SetInt(firstId + int64(idx))
				}
			}
			break
		}
	}
}

func Insert[T any](rows ...T) *IntoGather[T] {
	if len(rows) == 0 {
		panic(errs.New("Nothing to insert"))
	}
	return &IntoGather[T]{
		rows: rows,
	}
}

func InsertDO[T DO](rows ...T) *InsertDsl[T] {
	if len(rows) == 0 {
		panic(errs.New("Nothing to insert"))
	}
	return Insert(rows...).Into(rows[0].TableName())
}

type IntoGather[T any] struct {
	rows []T
}

func (i *IntoGather[T]) Into(table string) *InsertDsl[T] {
	return i.IntoT(Tbl(table))
}

func (i *IntoGather[T]) IntoT(table *Table) *InsertDsl[T] {
	return &InsertDsl[T]{
		rows:  i.rows,
		table: table,
	}
}
