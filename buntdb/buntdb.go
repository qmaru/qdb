package buntdb

import (
	"fmt"
	"sync"

	gobuntdb "github.com/tidwall/buntdb"
)

type Mode string

type Tx = gobuntdb.Tx
type SetOptions = gobuntdb.SetOptions

var IndexJson = gobuntdb.IndexJSON
var Desc = gobuntdb.Desc

type Buntdb struct {
	Mode     Mode
	FileName string
	once     sync.Once
	db       *gobuntdb.DB
	err      error
}

const (
	ModeFile   Mode = "file"
	ModeMemory Mode = "memory"
)

func NewFile(filename string) *Buntdb {
	return &Buntdb{
		Mode:     ModeFile,
		FileName: filename,
	}
}

func NewMemory() *Buntdb {
	return &Buntdb{
		Mode:     ModeMemory,
		FileName: ":memory:",
	}
}

func (b *Buntdb) Connect() (*gobuntdb.DB, error) {
	if b.db != nil {
		return b.db, nil
	}

	b.once.Do(func() {
		if b.FileName == "" {
			b.err = fmt.Errorf("file mode requires a non-empty filename")
			return
		}
		b.db, b.err = gobuntdb.Open(b.FileName)
	})
	return b.db, b.err
}

func (b *Buntdb) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *Buntdb) View(fn func(tx *Tx) error) error {
	db, err := b.Connect()
	if err != nil {
		return err
	}
	return db.View(fn)
}

func (b *Buntdb) Update(fn func(tx *Tx) error) error {
	db, err := b.Connect()
	if err != nil {
		return err
	}
	return db.Update(fn)
}

func (b *Buntdb) CreateIndex(name, pattern string, less ...func(a, b string) bool) error {
	db, err := b.Connect()
	if err != nil {
		return err
	}
	return db.CreateIndex(name, pattern, less...)
}

func (b *Buntdb) CreateSpatialIndex(name, pattern string, rect func(item string) (min, max []float64)) error {
	db, err := b.Connect()
	if err != nil {
		return err
	}
	return db.CreateSpatialIndex(name, pattern, rect)
}
