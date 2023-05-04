package postgresql

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/lib/pq"
)

type PostgreSQL struct {
	Host     string
	Port     int
	Username string
	Password string
	DBName   string
}

// Connect connecting a database
func (p *PostgreSQL) Connect() (*sql.DB, error) {
	dbInfo := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", p.Username, p.Password, p.Host, p.Port, p.DBName)
	db, err := sql.Open("postgres", dbInfo)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Exec Run a raw sql and return result
func (p *PostgreSQL) Exec(sql string, args ...any) (sql.Result, error) {
	pdb, err := p.Connect()
	if err != nil {
		return nil, err
	}
	defer pdb.Close()
	stmt, err := pdb.Prepare(sql)
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

// Exec Run a raw sql and return a row
func (p *PostgreSQL) QueryRow(sql string, args ...any) (*sql.Row, error) {
	pdb, err := p.Connect()
	if err != nil {
		return nil, err
	}
	defer pdb.Close()
	stmt, err := pdb.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	row := stmt.QueryRow(args...)
	return row, nil
}

// Query Run a raw sql and return some rows
func (p *PostgreSQL) Query(sql string, args ...any) (*sql.Rows, error) {
	pdb, err := p.Connect()
	if err != nil {
		return nil, err
	}
	defer pdb.Close()
	stmt, err := pdb.Prepare(sql)
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

// QueryOne Run a raw sql and return a row
func (p *PostgreSQL) QueryOne(sql string, args ...any) (*sql.Row, error) {
	pdb, err := p.Connect()
	if err != nil {
		return nil, err
	}
	defer pdb.Close()
	stmt, err := pdb.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	row := stmt.QueryRow(args...)
	return row, nil
}

// CreateTable create table using model
func (p *PostgreSQL) CreateTable(tables []any) error {
	pdb, err := p.Connect()
	if err != nil {
		return err
	}
	defer pdb.Close()
	for _, table := range tables {
		var buffer bytes.Buffer
		rType := reflect.TypeOf(table)
		rName := DBName(rType.Name())
		DBFiled(rType, &buffer)
		rFiled := buffer.Bytes()[0 : len(buffer.Bytes())-1]
		sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", rName, rFiled)
		_, err := pdb.Exec(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

// Comment add comment using model
func (p *PostgreSQL) Comment(tables []any) error {
	pdb, err := p.Connect()
	if err != nil {
		return err
	}
	defer pdb.Close()
	for _, table := range tables {
		var buffer bytes.Buffer
		rType := reflect.TypeOf(table)
		rName := DBName(rType.Name())
		DBComment(rType, &buffer)
		rFiled := buffer.Bytes()[0 : len(buffer.Bytes())-1]

		commentList := strings.Split(string(rFiled), ",")
		for _, comment := range commentList {
			rComment := strings.Split(comment, ":")
			sql := fmt.Sprintf(`COMMENT ON COLUMN %s.%s IS '%s';`, rName, rComment[0], rComment[1])
			_, err := pdb.Exec(sql)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// CreateIndex add index using model
func (p *PostgreSQL) CreateIndex(tables []any) error {
	pdb, err := p.Connect()
	if err != nil {
		return err
	}
	defer pdb.Close()
	for _, table := range tables {
		var buffer bytes.Buffer
		rType := reflect.TypeOf(table)
		rName := DBName(rType.Name())
		DBIndex(rType, &buffer)
		rFiled := buffer.Bytes()[0 : len(buffer.Bytes())-1]

		indexList := strings.Split(string(rFiled), ",")
		for _, index := range indexList {
			rIndex := strings.Split(index, ":")
			rFiled := rIndex[0]
			rType := rIndex[1]
			rIndexName := fmt.Sprintf("%s_%s_idx", rName, rIndex[0])
			sql := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s USING %s (%s);`, rIndexName, rName, rType, rFiled)
			_, err := pdb.Exec(sql)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Ping testing database
func (p *PostgreSQL) Ping() error {
	pdb, err := p.Connect()
	if err != nil {
		return err
	}
	defer pdb.Close()
	err = pdb.Ping()
	if err != nil {
		return err
	}
	return nil
}
