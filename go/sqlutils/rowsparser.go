package sqlutils

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type RowsParser struct {
	Rows    *sql.Rows
	columns map[int]Column
	Table   *Table
}

func NewRowsParser(rows *sql.Rows, table *Table) *RowsParser {
	return &RowsParser{Rows: rows, Table: table}
}

func (this *RowsParser) Parse(file *os.File) error {

	defer this.Rows.Close()

	buff := make([]interface{}, len(this.Table.Columns))
	data := make([]NullFieldData, len(this.Table.Columns))
	for i, _ := range buff {
		data[i].Column = this.Table.Columns[i]
		buff[i] = &data[i]
	}

	var rowsData []string
	var rowData []string

	rowCount := 0
	for this.Rows.Next() {
		if err := this.Rows.Scan(buff...); err != nil {
			panic(err)
		}

		rowData = rowData[:0]
		for _, val := range data {
			rowData = append(rowData, string(val.MarshalJSON()))
		}
		rowCount++
		rowsData = append(rowsData, fmt.Sprint("(", strings.Join(rowData, ","), ")"))
	}
	fmt.Fprintln(file, fmt.Sprintf("INSERT INTO %s VALUES", this.Table.Name))
	fmt.Fprintln(file, strings.Join(rowsData, ","), ";")

	return nil

}

func (this *RowsParser) Close() {
	this.Rows.Close()
}
