package boltdb

import (
	"bytes"
	"encoding/json"

	bolt "go.etcd.io/bbolt"
)

type BoltDB struct {
	FileName   string
	BucketName string
}

func New(filename, bucketname string) *BoltDB {
	return &BoltDB{
		FileName:   filename,
		BucketName: bucketname,
	}
}

// Connect Open and create
func (b *BoltDB) Connect() (*bolt.DB, error) {
	db, err := bolt.Open(b.FileName, 0600, nil)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// CreateBucket create a bucket
func (b *BoltDB) CreateBucket() error {
	db, err := b.Connect()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(b.BucketName))
		if err != nil {
			return err
		}
		return nil
	})
}

// GetBucket list buckets
func (b *BoltDB) GetBucket() (map[string]string, error) {
	db, err := b.Connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var data map[string]string
	tmp := make(map[string]string)
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(b.BucketName))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			tmp[string(k)] = string(v)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	jsonByte, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(jsonByte, &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Set create a key-value
func (b *BoltDB) Set(key, value []byte) error {
	db, err := b.Connect()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(b.BucketName))
		err := b.Put(key, value)
		return err
	})
}

// Get get value of key
func (b *BoltDB) Get(key []byte) ([]byte, error) {
	db, err := b.Connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var value []byte
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(b.BucketName))
		v := b.Get(key)
		value = append(value, v...)
		return nil
	})
	return value, err
}

// Scan get value of prefix or key
func (b *BoltDB) Scan(prefix, key []byte) ([]byte, error) {
	db, err := b.Connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tmps := make([]map[string]string, 0)
	err = db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(b.BucketName)).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if bytes.HasSuffix(k, key) {
				tmp := make(map[string]string)
				tmp[string(k)] = string(v)
				tmps = append(tmps, tmp)
			}
		}
		return nil
	})

	value, err := json.Marshal(tmps)
	if err != nil {
		return nil, err
	}
	return value, err
}

// Del delete a key
func (b *BoltDB) Del(key []byte) error {
	db, err := b.Connect()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(b.BucketName))
		err := b.Delete(key)
		return err
	})
}
