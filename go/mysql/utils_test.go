/*

 */
package mysql

import (
	"log"
	"testing"
)

func NewColumnTest(Name string, ColumnKey string, DataType string) *Column {
	column := Column{Name: "test"}
	return &column
}

func TestGetChunkSqlQuery(t *testing.T) {
	t.Log("Testing mysql package")

	columns := []*Column{
		&Column{Name: "pk", ColumnKey: "PRI", DataType: "int"},
		&Column{Name: "f1", DataType: "varchar"},
		&Column{Name: "f2", DataType: "varchar"},
		&Column{Name: "f3", DataType: "varchar"},
	}
	//columnsOrdinals := sqlutils.ColumnsMap

	pk := Column{Name: "pk", ColumnKey: "PRI", DataType: "int"}

	table := &Table{
		Name:    "table1",
		Schema:  "schema",
		Columns: columns,
		//ColumnsOrdinals: columnsOrdinals,
		PrimaryKey: &pk,
	}

	tables := []struct {
		table    *Table
		chunkMax interface{}
		offset   int64
		expect   string
	}{
		{table, 1570, 100, "SELECT pk FROM schema.table1 WHERE pk >= 1570 LIMIT 1 OFFSET 100"},
		{table, nil, 100, "SELECT pk FROM schema.table1 WHERE pk >= 0 LIMIT 1 OFFSET 100"},
	}
	for _, tt := range tables {
		query := GetChunkSqlQuery(tt.table, tt.chunkMax, tt.offset)
		log.Println(query)
		if query != tt.expect {
			t.Errorf("Error: got \n\"%s\" instead of \n\"%s\"", query, tt.expect)
		}
	}

}
