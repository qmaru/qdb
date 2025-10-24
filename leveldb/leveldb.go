package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDB struct {
	FileName string
	db       *leveldb.DB
}

func New(filename string) *LevelDB {
	return &LevelDB{
		FileName: filename,
	}
}

// Connect create database
func (ldb *LevelDB) Connect() (*leveldb.DB, error) {
	if ldb.db != nil {
		return ldb.db, nil
	}

	db, err := leveldb.OpenFile(ldb.FileName, nil)
	if err != nil {
		return nil, err
	}
	ldb.db = db
	return db, nil
}

func (ldb *LevelDB) Close() error {
	if ldb.db == nil {
		return nil
	}
	return ldb.db.Close()
}

// Set create key-value
func (ldb *LevelDB) Set(key, value []byte) error {
	db, err := ldb.Connect()
	if err != nil {
		return err
	}

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

	iter := db.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter.Release()

	data := make(map[string]any)
	for iter.Next() {
		k := string(iter.Key())
		v := iter.Value()
		vcopy := make([]byte, len(v))
		copy(vcopy, v)
		data[k] = vcopy
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}
	return data, nil
}

// Del delete a key
func (ldb *LevelDB) Del(key []byte) error {
	db, err := ldb.Connect()
	if err != nil {
		return err
	}

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

	ok, err := db.Has(key, nil)
	if err != nil {
		return false, err
	}
	return ok, nil
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
