package sqlitep

import (
	"database/sql"

	"github.com/qmaru/qdb/internal"

	_ "github.com/glebarez/go-sqlite"
)

type Tx = *sql.Tx
type Sqlite = internal.SqliteBase

// New creates a new SQLite instance using pure Go driver
func New(filename string) *Sqlite {
	return &internal.SqliteBase{
		FileName:   filename,
		DriverName: "sqlite",
	}
}
