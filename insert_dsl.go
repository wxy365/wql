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
	db := DataSource
	if len(optDb) > 0 {
		db = optDb[0]
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
	inserted := i.rows[len(i.rows)-1]
	val := reflect.ValueOf(inserted)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	if !val.CanAddr() {
		// non-addressable value (not a pointer), skip auto-increment write-back
		return nil
	}
	for j := 0; j < val.NumField(); j++ {
		if col, ok := val.Type().Field(j).Tag.Lookup("db"); ok && strings.HasSuffix(col, ";auto_incr") {
			rowIdx := len(i.rows) - 1
			val.Field(j).SetInt(id + int64(rowIdx))
			break
		}
	}
	return nil
}

func (i *InsertDsl[T]) ActionTx(ctx context.Context, tx *TX) error {
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
	inserted := i.rows[len(i.rows)-1]
	val := reflect.ValueOf(inserted)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	if !val.CanAddr() {
		return nil
	}
	for j := 0; j < val.NumField(); j++ {
		if col, ok := val.Type().Field(j).Tag.Lookup("db"); ok && strings.HasSuffix(col, ";auto_incr") {
			rowIdx := len(i.rows) - 1
			val.Field(j).SetInt(id + int64(rowIdx))
			break
		}
	}
	return nil
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
