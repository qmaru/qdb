# qdb

+ boltdb
+ leveldb
+ postgresql

## postgresql

```golang
type DemoModel struct {
    ID        uint64    `json:"id" db:"serial;PRIMARY KEY" comment:"ID"`
    CreatedAt time.Time `json:"created_at" db:"timestamp;DEFAULT NULL" comment:"created time"`
    UpdatedAt time.Time `json:"updated_at" db:"timestamp;DEFAULT NULL" comment:"updated time"`
    DeletedAt time.Time `json:"deleted_at" db:"timestamp;DEFAULT NULL" comment:"deleted time"`
    State     bool      `json:"state" db:"boolean;DEFAULT true" comment:"status"`
    Remark    string    `json:"remark" db:"varchar;DEFAULT ''" comment:"remark"`
}
```
