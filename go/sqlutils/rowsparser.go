package sqlutils

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type RowsParser struct {
	Rows    *sql.Rows
	columns map[int]*sql.ColumnType
	Table   *Table
}

func NewRowsParser(rows *sql.Rows, table *Table) *RowsParser {
	return &RowsParser{Rows: rows, Table: table}
}

func (this *RowsParser) Parse(file *os.File) error {

	columns, _ := this.Rows.ColumnTypes()
	buff := make([]interface{}, len(columns))
	data := make([]interface{}, len(columns))
	for i, _ := range buff {
		buff[i] = &data[i]
	}
	buffer := bufio.NewWriter(file)
	firstRow := true
	var err error
	buffer.WriteString(fmt.Sprintf("INSERT INTO %s VALUES \n(", this.Table.GetEscapedFullName()))
	for this.Rows.Next() {

		err = this.Rows.Scan(buff...)

		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		if err != nil {
			fmt.Println("error:", err)
		}
		if firstRow == false {
			buffer.WriteString("),\n(")
		} else {
			firstRow = false
		}

		max := len(data)
		for i, d := range data {

			switch d.(type) {
			case []byte:
				buffer.Write([]byte("'"))
				buffer.Write(parseString(d))
				buffer.Write([]byte("'"))
			case int64:
				buffer.WriteString(strconv.FormatInt(d.(int64), 10))
			case nil:
				buffer.Write([]byte("NULL"))
			case time.Time:
				buffer.WriteString(d.(time.Time).Format("2009-09-08 03:05:30.000000"))
			default:
				buffer.Write(d.([]byte))
			}
			if i != max-1 {
				buffer.Write([]byte(","))
			}
		}
	}
	buffer.WriteString(");\n")
	buffer.Flush()
	return nil
}

func (this *RowsParser) Close() {
	this.Rows.Close()
}
