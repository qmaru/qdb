package qdb

import (
	"testing"
)

func TestPostgresql(t *testing.T) {
	psql := NewPostgreSQL("127.0.0.1", 5432, "qmaru", "123456", "qmaru")
	err := psql.Ping()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoltDB(t *testing.T) {
	bdb := NewBoltDB("qmaru.db", "qmaru")
	_, err := bdb.Connect()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLevelDB(t *testing.T) {
	ldb := NewLevelDB("qmaru")
	_, err := ldb.Connect()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSqlite(t *testing.T) {
	sq3 := NewSqlite(":memory:")
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
	sq3p := NewSqlitep(":memory:")
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
