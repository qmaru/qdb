package rdb

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

// DBName formating table name
//
//	eg: UserName -> user_name
func DBName(s string) string {
	data := make([]string, 0)
	ok := false
	ru := []rune(s)

	for i := 0; i < len(ru); i++ {
		d := ru[i]
		if unicode.IsUpper(d) && ok {
			data = append(data, "_")
		}

		if string(d) != "_" {
			ok = true
		}
		data = append(data, string(d))
	}
	name := strings.Join(data, "")
	return strings.ToLower(name)
}

// DBFiled Parse the json|db field of the model
//
//	eg: json:"id"
//	eg: db:"serial;PRIMARY KEY"
//	eg: db:"integer;DEFAULT 0"
func DBFiled(reflectType reflect.Type, buffer *bytes.Buffer) {
	if reflectType.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < reflectType.NumField(); i++ {
		jsonTag := reflectType.Field(i).Tag.Get("json")
		dbTag := reflectType.Field(i).Tag.Get("db")

		if jsonTag == "" && dbTag == "" {
			DBFiled(reflectType.Field(i).Type, buffer)
			continue
		}

		dbProfile := strings.Split(dbTag, ";")
		dbFiled := fmt.Sprintf("%s %s", jsonTag, strings.Join(dbProfile, " "))
		buffer.WriteString(dbFiled)
		buffer.WriteString(",")
	}
}

// DBComment Parse the json|comment field of the model
//
//	eg: json:"id"
//	eg: comment:"ID"`
func DBComment(reflectType reflect.Type, buffer *bytes.Buffer) {
	if reflectType.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < reflectType.NumField(); i++ {
		jsonTag := reflectType.Field(i).Tag.Get("json")
		commentTag := reflectType.Field(i).Tag.Get("comment")

		if commentTag == "" {
			DBComment(reflectType.Field(i).Type, buffer)
			continue
		}

		commentData := fmt.Sprintf("%s:%s", jsonTag, commentTag)
		buffer.WriteString(commentData)
		buffer.WriteString(",")
	}
}

// DBIndex Parse the json|index field of the model
//
//	index type: btree, hash, gist, spgist, gin
//	eg: json:"id"
//	eg: index:"btree"
//	eg: index:"btree|unique"
func DBIndex(reflectType reflect.Type, buffer *bytes.Buffer) {
	if reflectType.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < reflectType.NumField(); i++ {
		jsonTag := reflectType.Field(i).Tag.Get("json")
		indexTag := reflectType.Field(i).Tag.Get("index")

		if indexTag == "" {
			DBIndex(reflectType.Field(i).Type, buffer)
			continue
		}

		indexData := fmt.Sprintf("%s:%s", jsonTag, indexTag)
		buffer.WriteString(indexData)
		buffer.WriteString(",")
	}
}
