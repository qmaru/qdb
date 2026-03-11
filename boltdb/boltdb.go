package boltdb

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Tx = bolt.Tx

type TxBucket struct {
	bucket *bolt.Bucket
}

type BoltDB struct {
	FileName string
	Timeout  time.Duration
	db       *bolt.DB
	once     sync.Once
	err      error
}

type Bucket struct {
	db   *BoltDB
	name []byte
}

// Default timeout value (15 seconds)
const DefaultTimeout = 15 * time.Second

func New(filename string) *BoltDB {
	return &BoltDB{
		FileName: filename,
		Timeout:  DefaultTimeout,
	}
}

// SetTimeout sets the connection timeout (should be called before Connect)
func (b *BoltDB) SetTimeout(timeout time.Duration) *BoltDB {
	b.Timeout = timeout
	return b
}

// Connect open database
func (b *BoltDB) Connect() (*bolt.DB, error) {
	b.once.Do(func() {
		if b.FileName == "" {
			b.err = fmt.Errorf("filename is empty")
			return
		}
		b.db, b.err = bolt.Open(
			b.FileName,
			0600,
			&bolt.Options{Timeout: b.Timeout},
		)
	})
	return b.db, b.err
}

func (b *BoltDB) Close() error {
	if b.db == nil {
		return nil
	}
	err := b.db.Close()
	b.db = nil
	return err
}

// Bucket returns a bucket instance
func (b *BoltDB) Bucket(name string) *Bucket {
	return &Bucket{
		db:   b,
		name: []byte(name),
	}
}

// Begin transaction
func (b *BoltDB) Begin(writeable bool) (*Tx, func() error, error) {
	db, err := b.Connect()
	if err != nil {
		return nil, nil, err
	}

	tx, err := db.Begin(writeable)
	if err != nil {
		return nil, nil, err
	}

	done := func() error {
		if writeable {
			return tx.Commit()
		}
		return tx.Rollback()
	}

	return tx, done, nil
}

func (b *BoltDB) View(fn func(*Tx) error) error {
	db, err := b.Connect()
	if err != nil {
		return err
	}
	return db.View(fn)
}

func (b *BoltDB) Update(fn func(*Tx) error) error {
	db, err := b.Connect()
	if err != nil {
		return err
	}
	return db.Update(fn)
}

// Create creates bucket if not exists
func (b *Bucket) Create() error {
	if len(b.name) == 0 {
		return fmt.Errorf("bucket name is empty")
	}

	return b.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(b.name)
		return err
	})
}

// Exists checks if bucket exists
func (b *Bucket) Exists() (bool, error) {
	if len(b.name) == 0 {
		return false, fmt.Errorf("bucket name is empty")
	}

	var ok bool
	err := b.db.View(func(tx *bolt.Tx) error {
		ok = tx.Bucket(b.name) != nil
		return nil
	})

	return ok, err
}

func (b *Bucket) View(fn func(*TxBucket) error) error {
	return b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.name)
		if bucket == nil {
			return fmt.Errorf("bucket %q not found", b.name)
		}
		return fn(&TxBucket{bucket: bucket})
	})
}

func (b *Bucket) Update(fn func(*TxBucket) error) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.name)
		if bucket == nil {
			return fmt.Errorf("bucket %q not found", b.name)
		}
		return fn(&TxBucket{bucket: bucket})
	})
}

// Set stores key-value
func (b *Bucket) Set(key, value []byte) error {
	return b.Update(func(tx *TxBucket) error {
		return tx.Set(key, value)
	})
}

// SetString helper
func (b *Bucket) SetString(key, value string) error {
	return b.Set([]byte(key), []byte(value))
}

// Get returns value
func (b *Bucket) Get(key []byte) ([]byte, error) {
	var v []byte
	err := b.View(func(tx *TxBucket) error {
		v = tx.Get(key)
		return nil
	})
	return v, err
}

// GetString helper
func (b *Bucket) GetString(key string) (string, error) {
	v, err := b.Get([]byte(key))
	return string(v), err
}

// ExistsKey checks if key exists
func (b *Bucket) ExistsKey(key []byte) (bool, error) {
	var ok bool
	err := b.View(func(tx *TxBucket) error {
		ok = tx.Get(key) != nil
		return nil
	})
	return ok, err
}

// Delete key
func (b *Bucket) Del(key []byte) error {
	return b.Update(func(tx *TxBucket) error {
		return tx.Del(key)
	})
}

// ForEach iterates prefix keys
func (b *Bucket) ForEach(prefix []byte, fn func(k, v []byte) error) error {
	return b.View(func(tx *TxBucket) error {
		c := tx.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			key := bytes.Clone(k)
			val := bytes.Clone(v)
			if err := fn(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeletePrefix deletes keys by prefix
func (b *Bucket) DeletePrefix(prefix []byte) error {
	return b.Update(func(tx *TxBucket) error {
		c := tx.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); {
			key := bytes.Clone(k)
			k, _ = c.Next()
			if err := tx.Del(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListKeyValues loads entire bucket
func (b *Bucket) ListKeyValues() (map[string]string, error) {
	res := make(map[string]string)
	err := b.View(func(tx *TxBucket) error {
		c := tx.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			res[string(k)] = string(v)
		}
		return nil
	})
	return res, err
}

func (b *TxBucket) Get(key []byte) []byte {
	v := b.bucket.Get(key)
	if v == nil {
		return nil
	}
	return bytes.Clone(v)
}

func (b *TxBucket) Set(key, value []byte) error {
	return b.bucket.Put(key, value)
}

func (b *TxBucket) Del(key []byte) error {
	return b.bucket.Delete(key)
}

func (b *TxBucket) Cursor() *bolt.Cursor {
	return b.bucket.Cursor()
}
