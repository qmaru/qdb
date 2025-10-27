package qdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/qmaru/qdb/badger"
	"github.com/qmaru/qdb/boltdb"
	"github.com/qmaru/qdb/cache/redis"
	"github.com/qmaru/qdb/leveldb"
	"github.com/qmaru/qdb/postgresql"
	"github.com/qmaru/qdb/sqlite"
	"github.com/qmaru/qdb/sqlitep"
)

func runConcurrentReaders(t *testing.T, readers int, work func(t *testing.T)) {
	t.Helper()
	for i := 0; i < readers; i++ {
		i := i
		t.Run(fmt.Sprintf("reader-%d", i), func(t *testing.T) {
			t.Parallel()
			work(t)
		})
	}
}

func TestBadgerDB(t *testing.T) {
	db := badger.New("qmaru", nil)
	db.SetMemoryMode(true)

	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("qmaru"), []byte("best"))
	})
	if err != nil {
		t.Fatal(err)
	}

	runConcurrentReaders(t, 10, func(t *testing.T) {
		if err := db.View(func(txn *badger.Txn) error {
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
		}); err != nil {
			t.Error(err)
		}
	})
}

func TestBoltDB(t *testing.T) {
	bucket := "qmaru"
	key := "qmaru"
	keyTx := "qmaru_tx"

	db := boltdb.New("qmaru.db", bucket)
	if err := db.CreateBucket(); err != nil {
		t.Fatalf("boltdb create bucket error: %v", err)
	}

	if ok, err := db.BucketExists(); err != nil {
		t.Fatalf("boltdb bucket %s does not exist", bucket)
	} else if !ok {
		t.Fatalf("boltdb bucket %s does not exist", bucket)
	}

	if err := db.Set([]byte(key), []byte("best")); err != nil {
		t.Fatal(err)
	}
	t.Logf("boltdb set key:%s value:%s\n", key, "best")

	val, err := db.Get([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("boltdb get key:%s value:%s\n", key, val)

	tx, commit, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	b := tx.Bucket([]byte(bucket))
	if b == nil {
		t.Fatalf("bucket %s does not exist", bucket)
	}
	if err := b.Put([]byte(keyTx), []byte("better")); err != nil {
		t.Fatal(err)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	t.Logf("boltdb transaction set key:%s value:%s\n", keyTx, "better")

	runConcurrentReaders(t, 10, func(t *testing.T) {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("boltdb get key:%s value:%s\n", key, val)

		val2, err := db.Get([]byte(keyTx))
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("boltdb get key:%s value:%s\n", keyTx, val2)

		results, err := db.ListBuckets()
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("boltdb list buckets count:%d\n", len(results))
	})
}

func TestLevelDB(t *testing.T) {
	key := "qmaru"

	ldb := leveldb.New("qmaru")

	err := ldb.Set([]byte(key), []byte("best"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("leveldb set key:%s value:%s\n", key, "best")

	val, err := ldb.Get([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("leveldb get key:%s value:%s\n", key, val)

	runConcurrentReaders(t, 10, func(t *testing.T) {
		val, err := ldb.Get([]byte(key))
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("leveldb get key:%s value:%s\n", key, val)
	})
}

func TestPostgresql(t *testing.T) {
	psql := postgresql.NewDefault("127.0.0.1", 5432, "qmaru", "123456", "qmaru")
	err := psql.Ping()
	if err != nil {
		t.Fatal(err)
	}

	runConcurrentReaders(t, 10, func(t *testing.T) {
		row, err := psql.QueryOne("SELECT version()")
		if err != nil {
			t.Error(err)
			return
		}
		var version string
		if err := row.Scan(&version); err != nil {
			t.Error(err)
			return
		}
		t.Logf("normal query: %s\n", version)
	})

	runConcurrentReaders(t, 5, func(t *testing.T) {
		err := psql.Transaction(func(tx postgresql.Tx) error {
			row, err := psql.QueryOneWithTx(tx, "SELECT version()")
			if err != nil {
				return err
			}
			var version string
			return row.Scan(&version)
		})
		if err != nil {
			t.Error(err)
			return
		}
		t.Log("transaction query ok")
	})
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

	runConcurrentReaders(t, 10, func(t *testing.T) {
		row, err := sql3.QueryOne("select sqlite_version()")
		if err != nil {
			t.Error(err)
			return
		}
		var v string
		if err := row.Scan(&v); err != nil {
			t.Error(err)
			return
		}
		t.Log(v)
	})
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

	runConcurrentReaders(t, 10, func(t *testing.T) {
		row, err := sql3p.QueryOne("select sqlite_version()")
		if err != nil {
			t.Error(err)
			return
		}
		var v string
		if err := row.Scan(&v); err != nil {
			t.Error(err)
			return
		}
		t.Log(v)
	})
}

func TestRedis(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewDefault(ctx, "127.0.0.1:6379", "", 0)
	err := rdb.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()

	key := "qdb:1"

	err = rdb.SetJSON(key, map[string]string{"name": "qmaru"}, 0)
	if err != nil {
		t.Fatal(err)
	}

	var result map[string]string
	err = rdb.GetJSON(key, &result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("redis get key:%s value:%v\n", key, result)
}
