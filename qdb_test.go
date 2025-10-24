package qdb

import (
	"testing"

	"github.com/qmaru/qdb/badger"
	"github.com/qmaru/qdb/boltdb"
	"github.com/qmaru/qdb/leveldb"
	"github.com/qmaru/qdb/postgresql"
	"github.com/qmaru/qdb/sqlite"
	"github.com/qmaru/qdb/sqlitep"
)

func TestBadgerDB(t *testing.T) {
	bdb := badger.New("qmaru", nil)
	_, err := bdb.Connect()
	if err != nil {
		t.Fatal(err)
	}

	bdb.SetMemoryMode(true)

	err = bdb.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("qmaru"), []byte("best"))
	})
	if err != nil {
		t.Fatal(err)
	}
	err = bdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("qmaru"))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		t.Logf("badger get key:qmaru value:%s\n", val)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

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
	psql := postgresql.NewDefault("127.0.0.1", 5432, "qmaru", "123456", "qmaru")
	err := psql.Ping()
	if err != nil {
		t.Fatal(err)
	}

	var version string
	// normal query
	row, err := psql.QueryOne("SELECT version()")
	if err != nil {
		t.Fatal(err)
	}

	err = row.Scan(&version)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("normal query: %s\n", version)

	// transaction query
	err = psql.Transaction(func(tx postgresql.Tx) error {
		row, err := psql.QueryOneWithTx(tx, "SELECT version()")
		if err != nil {
			t.Fatal(err)
		}
		return row.Scan(&version)
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("transaction query: %s\n", version)
}

func TestSqlite(t *testing.T) {
	sql3 := sqlite.New(":memory:")
	row, err := sql3.QueryOne("select sqlite_version()")
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err := row.Scan(&result); err != nil {
		t.Fatal(err)
	}
	t.Log(result)

	err = sql3.Transaction(func(tx sqlitep.Tx) error {
		row, err := sql3.QueryOneWithTx(tx, "select sqlite_version()")
		if err != nil {
			t.Fatal(err)
		}
		var result string
		if err := row.Scan(&result); err != nil {
			t.Fatal(err)
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	t.Log(result)
}

func TestSqlitep(t *testing.T) {
	sql3p := sqlitep.New(":memory:")
	row, err := sql3p.QueryOne("select sqlite_version()")
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err := row.Scan(&result); err != nil {
		t.Fatal(err)
	}
	t.Log(result)

	err = sql3p.Transaction(func(tx sqlitep.Tx) error {
		row, err := sql3p.QueryOneWithTx(tx, "select sqlite_version()")
		if err != nil {
			t.Fatal(err)
		}
		var result string
		if err := row.Scan(&result); err != nil {
			t.Fatal(err)
		}
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	t.Log(result)
}
