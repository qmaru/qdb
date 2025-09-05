package sqlite

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"github.com/qmaru/qdb/internal"
)

type Tx = *sql.Tx
type Sqlite = internal.SqliteBase

// New creates a new SQLite instance using CGO driver
func New(filename string) *Sqlite {
	return &internal.SqliteBase{
		FileName:   filename,
		DriverName: "sqlite3",
	}
}
