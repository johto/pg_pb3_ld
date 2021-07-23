package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/rand"
	"strconv"
)

type FuzzySchemaGenerator struct {
}

func NewFuzzySchemaGenerator() *FuzzySchemaGenerator {
	return &FuzzySchemaGenerator{}
}

func (sg *FuzzySchemaGenerator) generateSQLIdentifier() string {
	alphabet := []byte("abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")

	var length int
	for {
		length = int(rand.NormFloat64() * 10 + 5)
		if length > 0 && length <= MAX_IDENTIFIER_LENGTH {
			break
		}
	}
	value := ""
	for i := 0; i < length; i++ {
		value += string(alphabet[rand.Intn(len(alphabet))])
	}
	return value
}

func (sg *FuzzySchemaGenerator) GenerateSchema() *TestSchema {
	schema := &TestSchema{}

	for {
		schema.NumColumns = int(rand.NormFloat64() * 10 + 5)
		if schema.NumColumns > 0 && schema.NumColumns < 512 {
			break
		}
	}

	schema.TableName = sg.generateSQLIdentifier()
	schema.ColumnNames = make([]string, schema.NumColumns)
	schema.ColumnTypes = make([]SQLType, schema.NumColumns)
	columns := make(map[string]struct{})
	for i := 0; i < schema.NumColumns; i++ {
		for {
			schema.ColumnNames[i] = sg.generateSQLIdentifier()
			_, exists := columns[schema.ColumnNames[i]]
			if !exists {
				columns[schema.ColumnNames[i]] = struct{}{}
				break
			}
		}
		schema.ColumnTypes[i] = NewRandomSQLType()
	}

	return schema
}

type FuzzyTransactionGenerator struct {
	schema *TestSchema
	maxTransactions int
}

func NewFuzzyTransactionGenerator(schema *TestSchema) *FuzzyTransactionGenerator {
	maxTransactions := int(1024 + (rand.Float64() * 268435456))
	return &FuzzyTransactionGenerator{
		schema: schema,
		maxTransactions: maxTransactions,
	}
}

func (tg *FuzzyTransactionGenerator) generateSQLValue(t SQLType, sizeBudget int) SQLValue {
	if rand.Float64() < 0.05 {
		return SQL_NULL
	}

	switch t {
		case SQL_INTEGER:
			uvalue := rand.Uint32()
			datum := make([]byte, 4)
			binary.BigEndian.PutUint32(datum, uvalue)
			return SQLValue{
				Null: false,
				Binary: true,
				Datum: datum,
				TextRepresentation: strconv.FormatInt(int64(int32(uvalue)), 10),
			}
		case SQL_BIGINT:
			uvalue := rand.Uint64()
			datum := make([]byte, 8)
			binary.BigEndian.PutUint64(datum, uvalue)
			return SQLValue{
				Null: false,
				Binary: true,
				Datum: datum,
				TextRepresentation: strconv.FormatInt(int64(uvalue), 10),
			}
		case SQL_FLOAT4:
			uvalue := rand.Uint32()
			datum := make([]byte, 4)
			binary.BigEndian.PutUint32(datum, uvalue)
			return SQLValue{
				Null: false,
				Binary: true,
				Datum: datum,
			}
		case SQL_FLOAT8:
			uvalue := rand.Uint64()
			datum := make([]byte, 8)
			binary.BigEndian.PutUint64(datum, uvalue)
			return SQLValue{
				Null: false,
				Binary: true,
				Datum: datum,
			}
		case SQL_BYTEA:
			var length int
			for {
				if sizeBudget < 64 {
					length = 0
					break
				}
				length = int(rand.NormFloat64() * 10 + 100)
				if length < 0 {
					length = -length
				}
				if length < 67108864 {
					break
				}
			}
			datum := bytes.Repeat([]byte{'\xBB'}, length)
			return SQLValue{
				Null: false,
				Binary: true,
				Datum: datum,
			}
		default:
			panic(t)
	}
}

func (tg *FuzzyTransactionGenerator) GenerateTransaction() *TestTransaction {
	var numOperations int
	for {
		numOperations = int(rand.NormFloat64() * 10 + 5)
		if numOperations >= 1 {
			break
		}
	}

	operations := make([]TestOperation, numOperations)

	for i := range operations {
		// Don't test rows that take up more than 128MB of space.  Such rows
		// should be pretty uncommon, and we quickly run into issues with
		// postgres limitations.  There's still a possibility that we exceed
		// the size budget.  In that case we just start again from first
		// column.
		sizeBudget := 134217728
		usedSizeBudget := 0

		values := make([]SQLValue, tg.schema.NumColumns)
sizeBudgetExceeded:
		for n := range values {
			values[n] = tg.generateSQLValue(tg.schema.ColumnTypes[n], sizeBudget - usedSizeBudget)
			// FIXME this really depends on whether the values come out as
			// binary or text on the other end.
			usedSizeBudget += len(values[n].Datum)
			if usedSizeBudget >= sizeBudget {
				log.Printf(
					"size budget exceeded: %d > %d at column %d",
					usedSizeBudget,
					sizeBudget,
					n,
				)
				goto sizeBudgetExceeded
			}
		}

		operations[i] = &TestInsert{
			TableName: tg.schema.TableName,
			Values: values,
		}
	}

	txn := &TestTransaction{
		Operations: operations,
	}

	return txn
}
