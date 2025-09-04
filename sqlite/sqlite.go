package sqlite

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/qmaru/qdb/rdb"

	_ "github.com/mattn/go-sqlite3"
)

type Tx = *sql.Tx

type Sqlite struct {
	FileName string
}

// NewSqlite cgo-sqlite
func New(filename string) *Sqlite {
	return &Sqlite{
		FileName: filename,
	}
}

func (s *Sqlite) Connect() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", s.FileName)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (s *Sqlite) Exec(raw string, args ...any) (sql.Result, error) {
	sdb, err := s.Connect()
	if err != nil {
		return nil, err
	}
	defer sdb.Close()
	return sdb.Exec(raw, args...)
}

func (s *Sqlite) Query(sql string, args ...any) (*sql.Rows, error) {
	sdb, err := s.Connect()
	if err != nil {
		return nil, err
	}
	defer sdb.Close()
	return sdb.Query(sql, args...)
}

func (s *Sqlite) QueryOne(sql string, args ...any) (*sql.Row, error) {
	sdb, err := s.Connect()
	if err != nil {
		return nil, err
	}
	defer sdb.Close()
	return sdb.QueryRow(sql, args...), nil
}

// Transaction run transaction
func (s *Sqlite) Transaction(fn func(tx Tx) error) error {
	sdb, err := s.Connect()
	if err != nil {
		return err
	}
	defer sdb.Close()

	tx, err := sdb.Begin()
	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback() }()

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

// CreateTable create table using model
func (s *Sqlite) CreateTable(tables []any) error {
	sdb, err := s.Connect()
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
