package qdb

import (
	"github.com/qmaru/qdb/boltdb"
	"github.com/qmaru/qdb/leveldb"
	"github.com/qmaru/qdb/postgresql"
)

type BoltDB = *boltdb.BoltDB
type LevelDB = *leveldb.LevelDB
type PostgreSQL = *postgresql.PostgreSQL

func NewBoltDB(filename, bucketname string) *boltdb.BoltDB {
	return &boltdb.BoltDB{
		FileName:   filename,
		BucketName: bucketname,
	}
}

func NewLevelDB(filename string) *leveldb.LevelDB {
	return &leveldb.LevelDB{
		FileName: filename,
	}
}

func NewPostgreSQL(host string, port int, username, password, dbname string) *postgresql.PostgreSQL {
	return &postgresql.PostgreSQL{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		DBName:   dbname,
	}
}
