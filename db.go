package q

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/wxy365/basal/cfg/def"
	"github.com/wxy365/basal/errs"
)

func init() {
	if def.HasDefault() {
		dbCfg, err := def.GetObj[DBCfg]("database")
		if err == nil && dbCfg.Dsn != "" {
			dbType := ParseDbType(dbCfg.Driver)
			sqlDb, err := sql.Open(dbCfg.Driver, dbCfg.Driver)
			if err != nil {
				panic("illegal driver or datasource name")
			}
			if err = sqlDb.Ping(); err != nil {
				panic("cannot connect to database: " + err.Error())
			}
			DataSource = &DB{
				dbType: dbType,
				DB:     sqlDb,
			}
		}
	}
}

var DataSource *DB

type DBCfg struct {
	Driver             string `json:"driver"`
	Dsn                string `json:"dsn"`
	MaxOpenConns       int    `json:"max_open_conns"`
	MaxIdleConns       int    `json:"max_idle_conns"`
	ConnMaxIdleSeconds int    `json:"conn_max_idle_seconds"`
	ConnMaxLifeSeconds int    `json:"conn_max_life_seconds"`
}

type DB struct {
	*sql.DB
	dbType DbType
}

func Open(dbType DbType, dsn string) (*DB, error) {
	db, err := sql.Open(dbType.driverName(), dsn)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	ds := &DB{
		db,
		dbType,
	}
	if DataSource == nil {
		DataSource = ds
	}
	return ds, nil
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

func BeginTx() (*TX, error) {
	if DataSource == nil {
		return nil, errs.New("Default data source not configured")
	}
	return DataSource.BeginTx()
}

type TX struct {
	*sql.Tx
	dbType DbType
}

type DbType uint8

func ParseDbType(driver string) DbType {
	switch driver {
	case "mysql":
		return MySQL
	case "mariadb":
		return MariaDB
	case "postgres":
		return PostgreSQL
	case "sqlite3":
		return SQLite
	case "sqlserver":
		return SQLServer
	case "oracle":
		return Oracle
	default:
		panic("Unknown driver name")
	}
}

func (d DbType) driverName() string {
	switch d {
	case MySQL:
		return "mysql"
	case MariaDB:
		return "mariadb"
	case PostgreSQL:
		return "postgres"
	case SQLite:
		return "sqlite3"
	case SQLServer:
		return "sqlserver"
	case Oracle:
		return "oracle"
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
	MariaDB
	PostgreSQL
	SQLite
	SQLServer
	Oracle
)
