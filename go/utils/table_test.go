package utils

import "testing"

var table1 = &Table{
	name:       "table1",
	schema:     "schema1",
	IsLocked:   false,
	primaryKey: "pk",
}
var table2 = &Table{
	name:       "table2",
	schema:     "schema2",
	IsLocked:   false,
	primaryKey: "pk",
}
var table3 = &Table{
	name:      "table3",
	schema:    "schema3",
	IsLocked:  false,
	uniqueKey: "uk",
}

func TestTable(t *testing.T) {
	tables := []struct {
		table             *Table
		name              string
		schema            string
		fullname          string
		unescapedfullname string
		pkOrUk            string
	}{
		{table1, "`table1`", "`schema1`", "`schema1`.`table1`", "schema1.table1", "pk"},
		{table2, "`table2`", "`schema2`", "`schema2`.`table2`", "schema2.table2", "pk"},
		{table3, "`table3`", "`schema3`", "`schema3`.`table3`", "schema3.table3", "uk"},
	}

	for _, tt := range tables {
		if tt.table.GetName() != tt.name {
			t.Fatalf("Table name is %s and we expect %s.", tt.table.GetName(), tt.name)
		}
		if tt.table.GetSchema() != tt.schema {
			t.Fatalf("Table schema is %s and we expect %s.", tt.table.GetSchema(), tt.schema)
		}
		if tt.table.GetFullName() != tt.fullname {
			t.Fatalf("Table fullname is %s and we expect %s.",
				tt.table.GetFullName(), tt.fullname)
		}
		if tt.table.GetUnescapedFullName() != tt.unescapedfullname {
			t.Fatalf("Table unescaped fullname is %s and we expect %s.",
				tt.table.GetUnescapedFullName(), tt.unescapedfullname)
		}
		if tt.table.GetPrimaryOrUniqueKey() != tt.pkOrUk {
			t.Fatalf("Table primary or unique key is %s and we expect %s.",
				tt.table.GetPrimaryOrUniqueKey(), tt.pkOrUk)
		}
	}

}
