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
