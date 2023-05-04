package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDB struct {
	FileName string
}

// Connect create database
func (ldb *LevelDB) Connect() (*leveldb.DB, error) {
	db, err := leveldb.OpenFile(ldb.FileName, nil)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Set create key-value
func (ldb *LevelDB) Set(key, value []byte) error {
	db, err := ldb.Connect()
	if err != nil {
		return err
	}
	defer db.Close()
	err = db.Put(key, value, nil)
	if err != nil {
		return err
	}
	return nil
}

// Get get value of key
func (ldb *LevelDB) Get(key []byte) ([]byte, error) {
	db, err := ldb.Connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	value, err := db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// GetBatch get some key with prefix
func (ldb *LevelDB) GetBatch(keyPrefix string) (map[string]any, error) {
	db, err := ldb.Connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	iter := db.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)

	data := make(map[string]any)
	for iter.Next() {
		key := string(iter.Key())
		data[key] = iter.Value()
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return nil, err
	}
	return data, err
}

// Del delete a key
func (ldb *LevelDB) Del(key []byte) error {
	db, err := ldb.Connect()
	if err != nil {
		return err
	}
	defer db.Close()
	err = db.Delete(key, nil)
	if err != nil {
		return err
	}
	return nil
}

// Check check a key
func (ldb *LevelDB) Check(key []byte) (bool, error) {
	db, err := ldb.Connect()
	if err != nil {
		return false, err
	}
	defer db.Close()
	if ok, err := db.Has(key, nil); !ok {
		return false, err
	}
	return true, nil
}

// Batch create a batch instance
func (ldb *LevelDB) Batch() (*leveldb.DB, *leveldb.Batch, error) {
	db, err := ldb.Connect()
	if err != nil {
		return nil, nil, err
	}
	batch := new(leveldb.Batch)
	return db, batch, nil
}

// Iter create a iterator
func (ldb *LevelDB) Iter() (*leveldb.DB, iterator.Iterator, error) {
	db, err := ldb.Connect()
	if err != nil {
		return nil, nil, err
	}
	iter := db.NewIterator(nil, nil)
	return db, iter, nil
}
