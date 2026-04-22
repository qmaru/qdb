package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/qmaru/qdb/rdb"

	_ "github.com/go-sql-driver/mysql"
)

type Tx = *sql.Tx

type DBExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

type MySQLOptions struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime int
}

type MySQL struct {
	Host     string
	Port     int
	Username string
	Password string
	DBName   string
	Options  *MySQLOptions
	db       *sql.DB
}

func NewMySQLOptions() MySQLOptions {
	return MySQLOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 30,
	}
}

func New(host string, port int, username, password, dbname string, options *MySQLOptions) *MySQL {
	if options == nil {
		opts := NewMySQLOptions()
		options = &opts
	}

	return &MySQL{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		DBName:   dbname,
		Options:  options,
	}
}

func NewDefault(host string, port int, username, password, dbname string) *MySQL {
	opts := NewMySQLOptions()
	return &MySQL{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		DBName:   dbname,
		Options:  &opts,
	}
}

func (m *MySQL) Connect() error {
	if m.db == nil {
		if m.Options == nil {
			opts := NewMySQLOptions()
			m.Options = &opts
		}

		dbInfo := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			m.Username, m.Password, m.Host, m.Port, m.DBName)
		db, err := sql.Open("mysql", dbInfo)
		if err != nil {
			return fmt.Errorf("could not open database: %v", err)
		}

		db.SetMaxOpenConns(m.Options.MaxOpenConns)
		db.SetMaxIdleConns(m.Options.MaxIdleConns)
		db.SetConnMaxLifetime(time.Duration(m.Options.ConnMaxLifetime) * time.Minute)

		if err := db.Ping(); err != nil {
			return fmt.Errorf("could not ping database: %v", err)
		}

		m.db = db
	}

	return nil
}

func (m *MySQL) Close() {
	if m.db != nil {
		m.db.Close()
		m.db = nil
	}
}

func (m *MySQL) Stats() sql.DBStats {
	if m.db != nil {
		_ = m.Connect()
		if m.db == nil {
			return sql.DBStats{}
		}
	}
	return m.db.Stats()
}

func (m *MySQL) getExecutor(tx Tx) (DBExecutor, error) {
	if tx != nil {
		return tx, nil
	}

	if err := m.Connect(); err != nil {
		return nil, err
	}
	return m.db, nil
}

func (m *MySQL) ExecWithTx(tx Tx, sql string, args ...any) (sql.Result, error) {
	executor, err := m.getExecutor(tx)
	if err != nil {
		return nil, err
	}
	return executor.Exec(sql, args...)
}

func (m *MySQL) QueryWithTx(tx Tx, sql string, args ...any) (*sql.Rows, error) {
	executor, err := m.getExecutor(tx)
	if err != nil {
		return nil, err
	}
	return executor.Query(sql, args...)
}

// QueryOneWithTx Run a raw sql with Tx and return a row
func (m *MySQL) QueryOneWithTx(tx Tx, sql string, args ...any) (*sql.Row, error) {
	executor, err := m.getExecutor(tx)
	if err != nil {
		return nil, err
	}
	return executor.QueryRow(sql, args...), nil
}

// Exec Run a raw sql and return result
func (m *MySQL) Exec(sql string, args ...any) (sql.Result, error) {
	return m.ExecWithTx(nil, sql, args...)
}

// Query Run a raw sql and return some rows
func (m *MySQL) Query(sql string, args ...any) (*sql.Rows, error) {
	return m.QueryWithTx(nil, sql, args...)
}

// QueryOne Run a raw sql and return a row
func (m *MySQL) QueryOne(sql string, args ...any) (*sql.Row, error) {
	return m.QueryOneWithTx(nil, sql, args...)
}

// Transaction run transaction
func (m *MySQL) Transaction(fn func(tx Tx) error) error {
	if err := m.Connect(); err != nil {
		return err
	}

	tx, err := m.db.Begin()
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
func (m *MySQL) CreateTable(tables []any) error {
	if err := m.Connect(); err != nil {
		return err
	}

	for _, table := range tables {
		var fieldBuf bytes.Buffer
		var indexBuf bytes.Buffer

		rType := reflect.TypeOf(table)
		rName := rdb.DBName(rType.Name())

		rdb.DBFiled(rType, &fieldBuf)
		rdb.DBIndex(rType, &indexBuf)

		fields := strings.TrimRight(fieldBuf.String(), ",")
		indexes := strings.TrimRight(indexBuf.String(), ",")

		sql := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS `%s` (%s%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
			rName, fields, func() string {
				if indexes != "" {
					return "," + indexes
				}
				return ""
			}(),
		)

		_, err := m.Exec(sql)
		if err != nil {
			return err
		}
	}

	return nil
}

// Ping testing database
func (m *MySQL) Ping() error {
	if err := m.Connect(); err != nil {
		return err
	}

	err := m.db.Ping()
	if err != nil {
		return fmt.Errorf("ping failed: %v", err)
	}

	return nil
}
