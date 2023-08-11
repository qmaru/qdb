package qdb

import (
	"testing"

	"github.com/qmaru/qdb/boltdb"
	"github.com/qmaru/qdb/leveldb"
	"github.com/qmaru/qdb/postgresql"
	"github.com/qmaru/qdb/sqlite"
	"github.com/qmaru/qdb/sqlitep"
)

func TestBoltDB(t *testing.T) {
	bdb := boltdb.New("qmaru.db", "qmaru")
	_, err := bdb.Connect()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLevelDB(t *testing.T) {
	ldb := leveldb.New("qmaru")
	_, err := ldb.Connect()
	if err != nil {
		t.Fatal(err)
	}
}

func TestPostgresql(t *testing.T) {
	psql := postgresql.New("127.0.0.1", 5432, "qmaru", "123456", "qmaru")
	err := psql.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSqlite(t *testing.T) {
	sq3 := sqlite.New(":memory:")
	db, err := sq3.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := db.QueryRow("select sqlite_version()")
	var result string
	err = row.Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(result)
}

func TestSqlitep(t *testing.T) {
	sq3p := sqlitep.New(":memory:")
	db, err := sq3p.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := db.QueryRow("select sqlite_version()")
	var result string
	err = row.Scan(&result)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(result)
}
