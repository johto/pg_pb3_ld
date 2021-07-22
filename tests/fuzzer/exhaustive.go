package main

import (
	"strings"
)

const MAX_IDENTIFIER_LENGTH int = 63

type ExhaustiveSchemaGenerator struct {
	numColumns int
	tableNameLength int
	columnNameLengths []int
}

type ExhaustiveTransactionGenerator struct {
	done bool

	schema *TestSchema

	valueGenerators []exhaustiveSQLValueGenerator
	lastGeneratedValues []SQLValue
}

type exhaustiveSQLValueGenerator interface {
	done() bool
	generateValue() SQLValue
	reset()
}

type exhaustiveByteaGenerator struct {
	length int
}

func newExhaustiveByteaGenerator() *exhaustiveByteaGenerator {
	return &exhaustiveByteaGenerator{
		length: -1,
	}
}

func (g *exhaustiveByteaGenerator) done() bool {
	return g.length > 3
}

func (g *exhaustiveByteaGenerator) generateValue() SQLValue {
	if g.done() {
		panic("done")
	}

	var value SQLValue
	if g.length == -1 {
		value = SQLValue{
			Null: true,
			Binary: false,
			Datum: nil,
		}
	} else if g.length >= 0 {
		datum := make([]byte, g.length)
		for i := range datum {
			datum[i] = '\xDE';
		}
		value = SQLValue{
			Null: false,
			Binary: true,
			Datum: datum,
		}
	} else {
		panic(g.length)
	}

	g.length++

	return value
}

func (g *exhaustiveByteaGenerator) reset() {
	g.length = -1
}

func NewExhaustiveSchemaGenerator() *ExhaustiveSchemaGenerator {
	return &ExhaustiveSchemaGenerator{
		numColumns: 0,
		tableNameLength: 1,
		columnNameLengths: nil,
	}
}

func (sg *ExhaustiveSchemaGenerator) generateTableName(length int) string {
	return (strings.Repeat("xhaustive", 8))[:length]
}

func (sg *ExhaustiveSchemaGenerator) generateColumnName(idx int, length int) string {
	alphabet := []byte("abcdefg0123456789")
	if idx >= len(alphabet) {
		panic(idx)
	}
	return strings.Repeat(string(alphabet[idx]), length)
}

func (sg *ExhaustiveSchemaGenerator) GenerateSchema() *TestSchema {
	if sg.columnNameLengths == nil {
		sg.columnNameLengths = make([]int, sg.numColumns)
		for i := 0; i < sg.numColumns; i++ {
			sg.columnNameLengths[i] = 1
		}
	}

	schema := &TestSchema{}
	schema.TableName = sg.generateTableName(sg.tableNameLength)
	schema.NumColumns = sg.numColumns
	for i, l := range sg.columnNameLengths {
		schema.ColumnNames = append(schema.ColumnNames, sg.generateColumnName(i, l))
		schema.ColumnTypes = append(schema.ColumnTypes, SQL_BYTEA)
	}

	exhaustedColumnNameLengths := true
	for i := range sg.columnNameLengths {
		sg.columnNameLengths[i] = sg.columnNameLengths[i] + 1
		if sg.columnNameLengths[i] <= MAX_IDENTIFIER_LENGTH {
			exhaustedColumnNameLengths = false
			break
		} else {
			sg.columnNameLengths[i] = 1
		}
	}

	if exhaustedColumnNameLengths {
		sg.numColumns++
		sg.columnNameLengths = nil
		if sg.numColumns > 2 {
			sg.numColumns = 0

			sg.tableNameLength++
			if sg.tableNameLength > MAX_IDENTIFIER_LENGTH {
				sg.tableNameLength = 1
				sg.numColumns++
			}
		}
	}

	return schema
}

func NewExhaustiveTransactionGenerator(schema *TestSchema) *ExhaustiveTransactionGenerator {
	valueGenerators := make([]exhaustiveSQLValueGenerator, len(schema.ColumnTypes))
	for i, typ := range schema.ColumnTypes {
		switch typ {
			case SQL_INTEGER:
				panic("SQL_INTEGER")
			case SQL_BYTEA:
				valueGenerators[i] = newExhaustiveByteaGenerator()
			default:
				panic(typ)
		}
	}
	return &ExhaustiveTransactionGenerator{
		done: false,
		schema: schema,
		valueGenerators: valueGenerators,
		lastGeneratedValues: nil,
	}
}

func (tg *ExhaustiveTransactionGenerator) GenerateTransaction() *TestTransaction {
	if tg.done {
		return nil
	}
	if tg.lastGeneratedValues == nil {
		tg.lastGeneratedValues = make([]SQLValue, len(tg.valueGenerators))
		for i, gen := range tg.valueGenerators {
			tg.lastGeneratedValues[i] = gen.generateValue()
		}
	}

	copiedValues := append([]SQLValue(nil), tg.lastGeneratedValues...)
	operations := []TestOperation{
		&TestInsert{
			TableName: tg.schema.TableName,
			Values: copiedValues,
		},
	}

	txn := &TestTransaction{
		Operations: operations,
	}

	exhaustedGenerators := true
	for i, gen := range tg.valueGenerators {
		if !gen.done() {
			tg.lastGeneratedValues[i] = gen.generateValue()
			exhaustedGenerators = false
			break
		} else {
			gen.reset()
			tg.lastGeneratedValues[i] = gen.generateValue()
		}
	}
	if exhaustedGenerators {
		tg.done = true
	}

	return txn
}
