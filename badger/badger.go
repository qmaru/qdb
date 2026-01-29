package badger

import (
	"sync"

	gobadger "github.com/dgraph-io/badger/v4"
)

type Options = gobadger.Options
type Logger = gobadger.Logger
type Txn = gobadger.Txn

type BadgerDB struct {
	FileName       string
	Options        *Options
	memoryMode     bool
	encryptionKey  []byte
	indexCacheSize int64
	logger         gobadger.Logger
	once           sync.Once
	db             *gobadger.DB
	err            error
}

func New(filename string, options *Options) *BadgerDB {
	return &BadgerDB{
		FileName: filename,
		Options:  options,
	}
}

func (b *BadgerDB) SetLogger(logger Logger) {
	b.logger = logger
}

func (b *BadgerDB) SetMemoryMode(memoryMode bool) {
	b.memoryMode = memoryMode
}

func (b *BadgerDB) SetEncryption(key []byte, cacheSize int64) {
	b.encryptionKey = key
	b.indexCacheSize = cacheSize
}

// Connect Open and create
func (b *BadgerDB) Connect() (*gobadger.DB, error) {
	b.once.Do(func() {
		var opts gobadger.Options
		if b.Options != nil {
			opts = *b.Options
		} else {
			opts = gobadger.DefaultOptions(b.FileName)
		}

		if b.memoryMode {
			opts = opts.WithInMemory(true)
			opts.Dir = ""
			opts.ValueDir = ""
			opts = opts.WithLogger(b.logger)
		}

		if b.encryptionKey != nil {
			opts = opts.WithEncryptionKey(b.encryptionKey)
			if b.indexCacheSize > 0 {
				opts = opts.WithIndexCacheSize(b.indexCacheSize)
			}
		}

		b.db, b.err = gobadger.Open(opts)
	})

	return b.db, b.err
}

func (b *BadgerDB) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *BadgerDB) View(fn func(txn *Txn) error) error {
	db, err := b.Connect()
	if err != nil {
		return err
	}
	return db.View(fn)
}

func (b *BadgerDB) Update(fn func(txn *Txn) error) error {
	db, err := b.Connect()
	if err != nil {
		return err
	}
	return db.Update(fn)
}

func (b *BadgerDB) Begin(writable bool) (*Txn, func() error, error) {
	db, err := b.Connect()
	if err != nil {
		return nil, nil, err
	}
	txn := db.NewTransaction(writable)
	commit := func() error {
		if err := txn.Commit(); err != nil {
			txn.Discard()
			return err
		}
		return nil
	}
	return txn, commit, nil
}
