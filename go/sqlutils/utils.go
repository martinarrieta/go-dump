package sqlutils

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

func RowToArray(rows *sql.Rows, columns []*Column) []NullFieldData {
	buff := make([]interface{}, len(columns))
	data := make([]NullFieldData, len(columns))

	for i, _ := range buff {
		data[i].Column = columns[i]
		buff[i] = &data[i]
	}
	rows.Scan(buff...)
	return data
}
