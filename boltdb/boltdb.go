package boltdb

import (
	"bytes"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Tx = bolt.Tx

type BoltDB struct {
	FileName   string
	BucketName string
	db         *bolt.DB
}

func New(filename, bucketname string) *BoltDB {
	return &BoltDB{
		FileName:   filename,
		BucketName: bucketname,
	}
}

// Connect Open and create
func (b *BoltDB) Connect() (*bolt.DB, error) {
	if b.db != nil {
		return b.db, nil
	}

	if b.FileName == "" {
		return nil, fmt.Errorf("filename is empty")
	}

	db, err := bolt.Open(b.FileName, 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		return nil, err
	}
	b.db = db
	return b.db, nil
}

func (b *BoltDB) Close() error {
	if b.db == nil {
		return nil
	}
	return b.db.Close()
}

// CreateBucket create a bucket
func (b *BoltDB) CreateBucket() error {
	if b.BucketName == "" {
		return fmt.Errorf("bucket name is empty")
	}

	db, err := b.Connect()
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(b.BucketName))
		return err
	})
}

func (b *BoltDB) BucketExists() (bool, error) {
	if b.BucketName == "" {
		return false, fmt.Errorf("bucket name is empty")
	}

	db, err := b.Connect()
	if err != nil {
		return false, err
	}

	var exists bool
	err = db.View(func(tx *bolt.Tx) error {
		bt := tx.Bucket([]byte(b.BucketName))
		exists = (bt != nil)
		return nil
	})
	return exists, err
}

// ListBuckets list buckets
func (b *BoltDB) ListBuckets() (map[string]string, error) {
	if b.BucketName == "" {
		return nil, fmt.Errorf("bucket name is empty")
	}

	db, err := b.Connect()
	if err != nil {
		return nil, err
	}

	tmp := make(map[string]string)
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.BucketName))
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", b.BucketName)
		}
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			tmp[string(k)] = string(v)
		}
		return nil
	})

	return tmp, err
}

// Set create a key-value
func (b *BoltDB) Set(key, value []byte) error {
	if b.BucketName == "" {
		return fmt.Errorf("bucket name is empty")
	}

	db, err := b.Connect()
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.BucketName))
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", b.BucketName)
		}
		return bucket.Put(key, value)
	})
}

// Get get value of key
func (b *BoltDB) Get(key []byte) ([]byte, error) {
	if b.BucketName == "" {
		return nil, fmt.Errorf("bucket name is empty")
	}

	db, err := b.Connect()
	if err != nil {
		return nil, err
	}

	var value []byte
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.BucketName))
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", b.BucketName)
		}
		v := bucket.Get(key)
		if v != nil {
			value = append(value, v...)
		}
		return nil
	})
	return value, err
}

// Scan get value of prefix or key
func (b *BoltDB) Scan(prefix, key []byte) ([]map[string]string, error) {
	if b.BucketName == "" {
		return nil, fmt.Errorf("bucket name is empty")
	}

	db, err := b.Connect()
	if err != nil {
		return nil, err
	}

	tmps := make([]map[string]string, 0)
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.BucketName))
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", b.BucketName)
		}
		c := bucket.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if bytes.HasSuffix(k, key) {
				tmp := make(map[string]string)
				tmp[string(k)] = string(v)
				tmps = append(tmps, tmp)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return tmps, nil
}

// Del delete a key
func (b *BoltDB) Del(key []byte) error {
	if b.BucketName == "" {
		return fmt.Errorf("bucket name is empty")
	}

	db, err := b.Connect()
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.BucketName))
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", b.BucketName)
		}
		return bucket.Delete(key)
	})
}

func (b *BoltDB) Begin(writeable bool) (*Tx, func() error, error) {
	db, err := b.Connect()
	if err != nil {
		return nil, nil, err
	}

	tx, err := db.Begin(writeable)
	if err != nil {
		return nil, nil, err
	}

	commit := func() error {
		if writeable {
			return tx.Commit()
		}
		return tx.Rollback()
	}
	return tx, commit, nil
}
