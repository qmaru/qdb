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
	Options  PostgreSQLOptions
	db       *sql.DB
}

func NewPostgreSQLOptions() PostgreSQLOptions {
	return PostgreSQLOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 30,
	}
}

func New(host string, port int, username, password, dbname string, options PostgreSQLOptions) *PostgreSQL {
	if options == (PostgreSQLOptions{}) {
		options = NewPostgreSQLOptions()
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
	return &PostgreSQL{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		DBName:   dbname,
		Options:  NewPostgreSQLOptions(),
	}
}

// Connect connecting a database
func (p *PostgreSQL) Connect() error {
	if p.db != nil {
		return nil
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
	return nil
}

func (p *PostgreSQL) Close() {
	if p.db != nil {
		p.db.Close()
		p.db = nil
	}
}

func (p *PostgreSQL) Stats() sql.DBStats {
	return p.db.Stats()
}

func (p *PostgreSQL) execSQL(sql string, args ...any) (sql.Result, error) {
	if err := p.Connect(); err != nil {
		return nil, err
	}

	stmt, err := p.db.Prepare(sql)
	if err != nil {
		return nil, fmt.Errorf("could not prepare statement: %v", err)
	}
	defer stmt.Close()

	return stmt.Exec(args...)
}

// Exec Run a raw sql and return result
func (p *PostgreSQL) Exec(sql string, args ...any) (sql.Result, error) {
	return p.execSQL(sql, args...)
}

// Query Run a raw sql and return some rows
func (p *PostgreSQL) Query(sql string, args ...any) (*sql.Rows, error) {
	if err := p.Connect(); err != nil {
		return nil, err
	}
	stmt, err := p.db.Prepare(sql)
	if err != nil {
		return nil, fmt.Errorf("could not prepare statement: %v", err)
	}
	defer stmt.Close()

	return stmt.Query(args...)
}

// QueryOne Run a raw sql and return a row
func (p *PostgreSQL) QueryOne(sql string, args ...any) (*sql.Row, error) {
	if err := p.Connect(); err != nil {
		return nil, err
	}

	stmt, err := p.db.Prepare(sql)
	if err != nil {
		return nil, fmt.Errorf("could not prepare statement: %v", err)
	}
	defer stmt.Close()
	return stmt.QueryRow(args...), nil
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

	p.Close()
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
	p.Close()
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
				indexSql = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s USING %s (%s vector_l2_ops);`, rIndexName, rName, indexTag, rFiled)
			} else {
				indexSql = fmt.Sprintf(`CREATE %s INDEX IF NOT EXISTS %s ON %s USING %s (%s);`, uniqueTag, rIndexName, rName, indexTag, rFiled)
			}
			_, err := p.Exec(indexSql)
			if err != nil {
				return err
			}
		}
	}
	p.Close()
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
