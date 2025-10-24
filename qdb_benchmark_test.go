package qdb

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qmaru/qdb/badger"
)

func newMemDB(name string) (*badger.BadgerDB, error) {
	db := badger.New(name, nil)
	db.SetMemoryMode(true)
	db.SetLogger(nil)
	if _, err := db.Connect(); err != nil {
		return nil, err
	}
	return db, nil
}

func BenchmarkBadger(b *testing.B) {
	db, err := newMemDB("qmaru_base")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("qmaru"), []byte("best"))
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte("qmaru"))
			if err != nil {
				return err
			}
			_, err = item.ValueCopy(nil)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBadger_Write(b *testing.B) {
	db, err := newMemDB("qmaru_write")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte("key_write_" + strconv.Itoa(i))
		val := []byte("value_write_" + strconv.Itoa(i))
		if err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, val)
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBadger_Read(b *testing.B) {
	const keys = 1000
	db, err := newMemDB("qmaru_read")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < keys; i++ {
			if err := txn.Set([]byte(fmt.Sprintf("rk_%d", i)), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		idx := i % keys
		if err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(fmt.Sprintf("rk_%d", idx)))
			if err != nil {
				return err
			}
			_, err = item.ValueCopy(nil)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBadger_WriteParallel(b *testing.B) {
	db, err := newMemDB("qmaru_write_par")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var counter uint64
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&counter, 1)
			key := []byte("wpar_" + strconv.FormatUint(id, 10))
			val := []byte("v")
			if err := db.Update(func(txn *badger.Txn) error {
				return txn.Set(key, val)
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBadger_ReadParallel(b *testing.B) {
	const keys = 10000
	db, err := newMemDB("qmaru_read_par")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < keys; i++ {
			if err := txn.Set([]byte(fmt.Sprintf("rpar_%d", i)), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := r.Intn(keys)
			if err := db.View(func(txn *badger.Txn) error {
				item, err := txn.Get([]byte(fmt.Sprintf("rpar_%d", idx)))
				if err != nil {
					return err
				}
				_, err = item.ValueCopy(nil)
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBadger_ReadWriteParallel(b *testing.B) {
	const keys = 10000
	db, err := newMemDB("qmaru_rw_par")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < keys; i++ {
			if err := txn.Set([]byte(fmt.Sprintf("rw_%d", i)), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	var wcounter uint64
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if r.Intn(4) == 0 {
				id := atomic.AddUint64(&wcounter, 1)
				key := []byte("rw_w_" + strconv.FormatUint(id, 10))
				if err := db.Update(func(txn *badger.Txn) error {
					return txn.Set(key, []byte("v"))
				}); err != nil {
					b.Fatal(err)
				}
			} else {
				idx := r.Intn(keys)
				if err := db.View(func(txn *badger.Txn) error {
					item, err := txn.Get([]byte(fmt.Sprintf("rw_%d", idx)))
					if err != nil {
						return err
					}
					_, err = item.ValueCopy(nil)
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
