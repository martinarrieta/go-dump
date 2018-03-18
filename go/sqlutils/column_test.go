/*

 */
package sqlutils

import (
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
		Name:       "table1",
		Schema:     "schema",
		PrimaryKey: &pk,
	}
	for _, column := range columns {
		table.AddColumn(column)
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
		query := tt.table.GetChunkSqlQuery(tt.chunkMax, tt.offset)
		if query != tt.expect {
			t.Errorf("Error: got \n\"%s\" instead of \n\"%s\"", query, tt.expect)
		}
	}

}

func TestGetSchameAndTable(t *testing.T) {
	table := &Table{
		Name:   "table1",
		Schema: "schema",
	}
	if table.GetSchameAndTable() != "schema.table1" {
		t.Errorf("Error: got \n\"%s\" instead of \n\"schema.table1\"", table.GetSchameAndTable())
	}
}
