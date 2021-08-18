package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	proto "github.com/golang/protobuf/proto"
	"strconv"
)

type TestInsert struct {
	TableName string
	Values []SQLValue
}

func (ti *TestInsert) Execute(schema *TestSchema, txn pgx.Tx) error {
	conn := txn.Conn().PgConn()

	sql := "INSERT INTO \"" + ti.TableName + "\" VALUES ("
	paramFormats := make([]int16, len(ti.Values))
	paramValues := make([][]byte, len(ti.Values))
	paramOids := make([]uint32, len(ti.Values))
	for i, val := range ti.Values {
		if val.Null {
			paramValues[i] = nil
		} else {
			paramValues[i] = val.Datum
		}
		if val.Binary {
			paramFormats[i] = 1
		} else {
			paramFormats[i] = 0
		}
		paramOids[i] = schema.ColumnTypes[i].Oid()
	}
	for i := range ti.Values {
		if i > 0 {
			sql += ", "
		}
		sql += "$" + strconv.Itoa(i + 1)
	}
	sql += ")"

	if len(ti.Values) == 0 {
		sql = "INSERT INTO \"" + ti.TableName + "\" DEFAULT VALUES"
	}

	res := conn.ExecParams(context.Background(), sql, paramValues, paramOids, paramFormats, nil)
	_, err := res.Close()
	if err != nil {
		return err
	}

	return nil
}

func (ti *TestInsert) ExpectedMessages(schema *TestSchema) []proto.Message {
	values := make([][]byte, len(ti.Values))
	// omit_nulls
	var typeOids []uint32
	nulls := make([]byte, len(ti.Values))
	// disabled
	formats := []byte(nil)
	for i, val := range ti.Values {
		if val.Null {
			values[i] = []byte{}
			nulls[i] = '\x01'
		} else {
			values[i] = val.Datum
			nulls[i] = '\x00'
			typeOids = append(typeOids, schema.ColumnTypes[i].Oid())
		}
	}
	id := &InsertDescription{
		Table: &TableDescription{
			SchemaName: "public",
			TableName: ti.TableName,
		},
		NewValues: &FieldSetDescription{
			Names: schema.ColumnNames,
			Values: values,
			TypeOids: typeOids,
			Nulls: nulls,
			Formats: formats,
		},
	}
	return []proto.Message{id}
}

func (ti *TestInsert) Describe() string {
	value := "Insert " + ti.TableName + " {\n"
	for i, val := range ti.Values {
		if i > 0 {
			value += ",\n"
		}
		if val.Null {
			value += "    nil"
		} else {
			value += fmt.Sprintf("    %q", val.Datum)
		}
	}
	value += "\n}"
	return value
}

