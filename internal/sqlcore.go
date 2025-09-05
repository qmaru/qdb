package internal

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/qmaru/qdb/rdb"
)

type Tx = *sql.Tx

type Connector interface {
	Connect() (*sql.DB, error)
}

type SqliteBase struct {
	FileName   string
	DriverName string
}

func (s *SqliteBase) Connect() (*sql.DB, error) {
	db, err := sql.Open(s.DriverName, s.FileName)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (s *SqliteBase) Exec(query string, args ...any) (sql.Result, error) {
	return Exec(s, query, args...)
}

func (s *SqliteBase) Query(query string, args ...any) (*sql.Rows, error) {
	return Query(s, query, args...)
}

func (s *SqliteBase) QueryOne(query string, args ...any) (*sql.Row, error) {
	return QueryOne(s, query, args...)
}

func (s *SqliteBase) QueryOneWithTx(tx Tx, query string, args ...any) (*sql.Row, error) {
	return QueryOneWithTx(s, tx, query, args...)
}

func (s *SqliteBase) QueryWithTx(tx Tx, query string, args ...any) (*sql.Rows, error) {
	return QueryWithTx(s, tx, query, args...)
}

func (s *SqliteBase) ExecWithTx(tx Tx, query string, args ...any) (sql.Result, error) {
	return ExecWithTx(s, tx, query, args...)
}

func (s *SqliteBase) Transaction(fn func(tx Tx) error) error {
	return Transaction(s, fn)
}

func (s *SqliteBase) CreateTable(tables []any) error {
	return CreateTable(s, tables)
}

func Exec(c Connector, query string, args ...any) (sql.Result, error) {
	db, err := c.Connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	return db.Exec(query, args...)
}

func Query(c Connector, query string, args ...any) (*sql.Rows, error) {
	db, err := c.Connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	return db.Query(query, args...)
}

func QueryOne(c Connector, query string, args ...any) (*sql.Row, error) {
	db, err := c.Connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	return db.QueryRow(query, args...), nil
}

func QueryOneWithTx(c Connector, tx Tx, query string, args ...any) (*sql.Row, error) {
	return tx.QueryRow(query, args...), nil
}

func QueryWithTx(c Connector, tx Tx, query string, args ...any) (*sql.Rows, error) {
	return tx.Query(query, args...)
}

func ExecWithTx(c Connector, tx Tx, query string, args ...any) (sql.Result, error) {
	return tx.Exec(query, args...)
}

func Transaction(c Connector, fn func(tx Tx) error) error {
	db, err := c.Connect()
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func CreateTable(c Connector, tables []any) error {
	sdb, err := c.Connect()
	if err != nil {
		return err
	}
	defer sdb.Close()
	for _, table := range tables {
		var buffer bytes.Buffer
		rType := reflect.TypeOf(table)
		rName := rdb.DBName(rType.Name())
		rdb.DBFiled(rType, &buffer)
		rFiled := buffer.Bytes()[0 : len(buffer.Bytes())-1]
		sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", rName, rFiled)
		_, err := sdb.Exec(sql)
		if err != nil {
			return err
		}
	}
	return nil
}
