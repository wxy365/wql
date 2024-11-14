package q

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type DB struct {
	*sql.DB
	dbType DbType
}

func Open(dbType DbType, dsn string) (*DB, error) {
	db, err := sql.Open(dbType.driverName(), dsn)
	if err != nil {
		return nil, err
	}
	return &DB{
		db,
		dbType,
	}, nil
}

func NewDB(db *sql.DB, dbType DbType) *DB {
	return &DB{
		db,
		dbType,
	}
}

func (d *DB) BeginTx() (*TX, error) {
	tx, err := d.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &TX{tx, d.dbType}, nil
}

type TX struct {
	*sql.Tx
	dbType DbType
}

type DbType uint8

func (d DbType) driverName() string {
	switch d {
	case MySQL:
		return "mysql"
	default:
		panic("Unknown database type")
	}
}

func (d DbType) escaper() func(string) string {
	switch d {
	case MySQL:
		return func(s string) string {
			return "`" + s + "`"
		}
	default:
		panic("This database type is not supported yet")
	}
}

const (
	_ DbType = iota
	MySQL
	//MariaDB
	//PostgreSQL
	//Oracle
)
