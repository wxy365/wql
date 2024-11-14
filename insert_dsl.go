package q

import (
	"context"
	"github.com/wxy365/basal/fn"
	"github.com/wxy365/basal/lei"
	"reflect"
	"strings"
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

func (i *InsertDsl[T]) Action(ctx context.Context, db *DB) error {
	renderCtx := &RenderCtx{
		dbType: db.dbType,
		cnt:    new(fn.Counter),
	}
	stm, params := i.Build().GetStatement(renderCtx).prepare()
	result, err := db.ExecContext(ctx, stm, params...)
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	inserted := i.rows[len(i.rows)-1]
	val := reflect.ValueOf(inserted)
	for j := 0; j < val.NumField(); j++ {
		if col, ok := val.Type().Field(j).Tag.Lookup("db"); ok && strings.HasSuffix(col, ":auto_incr") {
			val.Field(j).SetInt(id)
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
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	inserted := i.rows[len(i.rows)-1]
	val := reflect.ValueOf(inserted)
	for j := 0; j < val.NumField(); j++ {
		if col, ok := val.Type().Field(j).Tag.Lookup("db"); ok && strings.HasSuffix(col, ":auto_incr") {
			val.Field(j).SetInt(id)
			break
		}
	}
	return nil
}

func Insert[T any](rows ...T) *IntoGather[T] {
	if len(rows) == 0 {
		panic(lei.New("Nothing to insert"))
	}
	return &IntoGather[T]{
		rows: rows,
	}
}

func InsertDO[T DO](rows ...T) *InsertDsl[T] {
	if len(rows) == 0 {
		panic(lei.New("Nothing to insert"))
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
