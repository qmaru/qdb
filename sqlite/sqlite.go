package sqlite

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/qmaru/qdb/rdb"

	_ "github.com/mattn/go-sqlite3"
)

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
	stmt, err := sdb.Prepare(raw)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	result, err := stmt.Exec(args...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Sqlite) Query(sql string, args ...any) (*sql.Rows, error) {
	sdb, err := s.Connect()
	if err != nil {
		return nil, err
	}
	defer sdb.Close()
	stmt, err := sdb.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *Sqlite) QueryOne(sql string, args ...any) (*sql.Row, error) {
	sdb, err := s.Connect()
	if err != nil {
		return nil, err
	}
	defer sdb.Close()
	stmt, err := sdb.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	row := stmt.QueryRow(args...)
	return row, nil
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
