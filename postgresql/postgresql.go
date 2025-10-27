package postgresql

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/qmaru/qdb/rdb"

	_ "github.com/lib/pq"
)

type Tx = *sql.Tx

type DBExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

type PostgreSQLOptions struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime int
}

type PostgreSQL struct {
	Host     string
	Port     int
	Username string
	Password string
	DBName   string
	Options  *PostgreSQLOptions
	db       *sql.DB
}

func NewPostgreSQLOptions() PostgreSQLOptions {
	return PostgreSQLOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 30,
	}
}

func New(host string, port int, username, password, dbname string, options *PostgreSQLOptions) *PostgreSQL {
	if options == nil {
		opts := NewPostgreSQLOptions()
		options = &opts
	}

	return &PostgreSQL{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		DBName:   dbname,
		Options:  options,
	}
}

func NewDefault(host string, port int, username, password, dbname string) *PostgreSQL {
	opts := NewPostgreSQLOptions()
	return &PostgreSQL{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		DBName:   dbname,
		Options:  &opts,
	}
}

// Connect connecting a database
func (p *PostgreSQL) Connect() error {
	if p.db == nil {
		if p.Options == nil {
			opts := NewPostgreSQLOptions()
			p.Options = &opts
		}

		dbInfo := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", p.Username, p.Password, p.Host, p.Port, p.DBName)
		db, err := sql.Open("postgres", dbInfo)
		if err != nil {
			return fmt.Errorf("could not open database: %v", err)
		}

		db.SetMaxOpenConns(p.Options.MaxOpenConns)
		db.SetMaxIdleConns(p.Options.MaxIdleConns)
		db.SetConnMaxLifetime(time.Duration(p.Options.ConnMaxLifetime) * time.Minute)

		if err := db.Ping(); err != nil {
			return fmt.Errorf("could not ping database: %v", err)
		}

		p.db = db
	}

	return nil
}

func (p *PostgreSQL) Close() {
	if p.db != nil {
		p.db.Close()
		p.db = nil
	}
}

func (p *PostgreSQL) Stats() sql.DBStats {
	if p.db == nil {
		_ = p.Connect()
		if p.db == nil {
			return sql.DBStats{}
		}
	}
	return p.db.Stats()
}

func (p *PostgreSQL) getExecutor(tx Tx) (DBExecutor, error) {
	if tx != nil {
		return tx, nil
	}

	if err := p.Connect(); err != nil {
		return nil, err
	}
	return p.db, nil
}

// ExecWithTx Run a raw sql with Tx and return result
func (p *PostgreSQL) ExecWithTx(tx Tx, sql string, args ...any) (sql.Result, error) {
	executor, err := p.getExecutor(tx)
	if err != nil {
		return nil, err
	}
	return executor.Exec(sql, args...)
}

// QueryWithTx Run a raw sql with Tx and return some rows
func (p *PostgreSQL) QueryWithTx(tx Tx, sql string, args ...any) (*sql.Rows, error) {
	executor, err := p.getExecutor(tx)
	if err != nil {
		return nil, err
	}
	return executor.Query(sql, args...)
}

// QueryOneWithTx Run a raw sql with Tx and return a row
func (p *PostgreSQL) QueryOneWithTx(tx Tx, sql string, args ...any) (*sql.Row, error) {
	executor, err := p.getExecutor(tx)
	if err != nil {
		return nil, err
	}
	return executor.QueryRow(sql, args...), nil
}

// Exec Run a raw sql and return result
func (p *PostgreSQL) Exec(sql string, args ...any) (sql.Result, error) {
	return p.ExecWithTx(nil, sql, args...)
}

// Query Run a raw sql and return some rows
func (p *PostgreSQL) Query(sql string, args ...any) (*sql.Rows, error) {
	return p.QueryWithTx(nil, sql, args...)
}

// QueryOne Run a raw sql and return a row
func (p *PostgreSQL) QueryOne(sql string, args ...any) (*sql.Row, error) {
	return p.QueryOneWithTx(nil, sql, args...)
}

// Transaction run transaction
func (p *PostgreSQL) Transaction(fn func(tx Tx) error) error {
	if err := p.Connect(); err != nil {
		return err
	}

	tx, err := p.db.Begin()
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
func (p *PostgreSQL) CreateTable(tables []any) error {
	if err := p.Connect(); err != nil {
		return err
	}

	for _, table := range tables {
		var buffer bytes.Buffer
		rType := reflect.TypeOf(table)
		rName := rdb.DBName(rType.Name())
		rdb.DBFiled(rType, &buffer)
		rFiled := buffer.Bytes()[0 : len(buffer.Bytes())-1]
		sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", rName, rFiled)
		_, err := p.Exec(sql)
		if err != nil {
			return err
		}
	}

	return nil
}

// Comment add comment using model
func (p *PostgreSQL) Comment(tables []any) error {
	if err := p.Connect(); err != nil {
		return err
	}

	for _, table := range tables {
		var buffer bytes.Buffer
		rType := reflect.TypeOf(table)
		rName := rdb.DBName(rType.Name())
		rdb.DBComment(rType, &buffer)
		rFiled := buffer.Bytes()[0 : len(buffer.Bytes())-1]

		commentList := strings.Split(string(rFiled), ",")
		for _, comment := range commentList {
			rComment := strings.Split(comment, ":")
			sql := fmt.Sprintf(`COMMENT ON COLUMN %s.%s IS '%s';`, rName, rComment[0], rComment[1])
			_, err := p.Exec(sql)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// CreateIndex add index using model
func (p *PostgreSQL) CreateIndex(tables []any) error {
	if err := p.Connect(); err != nil {
		return err
	}

	for _, table := range tables {
		var buffer bytes.Buffer
		rType := reflect.TypeOf(table)
		rName := rdb.DBName(rType.Name())
		rdb.DBIndex(rType, &buffer)
		rFiled := buffer.Bytes()[0 : len(buffer.Bytes())-1]

		indexList := strings.Split(string(rFiled), ",")
		for _, index := range indexList {
			rIndex := strings.Split(index, ":")
			rFiled := rIndex[0]
			rType := rIndex[1]
			// rType with unique
			rTypeCheck := strings.Split(rType, "|")
			var indexTag string
			var uniqueTag string
			if len(rTypeCheck) == 2 {
				uniqueTag = rTypeCheck[1]
			}
			indexTag = rTypeCheck[0]

			rIndexName := fmt.Sprintf("%s_%s_idx", rName, rIndex[0])

			var indexSql string
			if indexTag == "hnsw" {
				// uniqueTag: vector_l2_ops | vector_ip_ops | vector_cosine_ops | vector_l1_ops | bit_hamming_ops | bit_jaccard_ops
				indexSql = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s USING %s (%s %s);`, rIndexName, rName, indexTag, rFiled, uniqueTag)
			} else {
				indexSql = fmt.Sprintf(`CREATE %s INDEX IF NOT EXISTS %s ON %s USING %s (%s);`, uniqueTag, rIndexName, rName, indexTag, rFiled)
			}
			_, err := p.Exec(indexSql)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Ping testing database
func (p *PostgreSQL) Ping() error {
	if err := p.Connect(); err != nil {
		return err
	}

	err := p.db.Ping()
	if err != nil {
		return fmt.Errorf("ping failed: %v", err)
	}

	return nil
}
