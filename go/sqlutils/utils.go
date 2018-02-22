package sqlutils

import (
	"github.com/outbrain/golib/log"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)


func NewTable(schema string, name string, db *sql.DB) *Table {

  query := `SELECT
		COLUMN_NAME,
		ORDINAL_POSITION,
		COLUMN_DEFAULT,
		IS_NULLABLE,
		DATA_TYPE,
		CHARACTER_MAXIMUM_LENGTH,
		CHARACTER_OCTET_LENGTH,
		NUMERIC_PRECISION,
		NUMERIC_SCALE,
		DATETIME_PRECISION,
		CHARACTER_SET_NAME,
		COLLATION_NAME,
		COLUMN_KEY,
		EXTRA
	 FROM information_schema.columns WHERE table_schema=? AND table_name=? ORDER BY ORDINAL_POSITION`

  rows, err := db.Query(query, schema, name)


  if err != nil && err != sql.ErrNoRows {
  	return nil
  }

	var columns []*Column
	columnsOrdinals := make(map[string]int)
	var pk *Column

  for rows.Next() {
		column := new(Column)
		temp := make([]interface{}, 4)
		err := rows.Scan(&column.Name, &column.OrdinalPosition, &column.Default, &temp[0],
			  &column.DataType, &column.CharacterMaxLength, &column.CharacterOctetLength,
			  &column.NumericPrecision, &column.NumericScale, &column.DateTimePrecision,
			  &column.CharacterSetName, &column.CollationName, &column.ColumnKey,
			  &column.Extra)

		if temp[0] == "YES" {
			column.IsNullable = true
		} else {
			column.IsNullable = false
		}

		if *column.ColumnKey == "PRI" {
			pk = column
		}

		if err != nil {
			log.Fatal("error", err.Error())
		}
		columns = append(columns, column)
		columnsOrdinals[column.Name] = column.OrdinalPosition - 1
  }



  table := &Table{
    Name:         		name,
    Schema:       		schema,
		Columns:					columns,
		ColumnsOrdinals: 	columnsOrdinals,
		PrimaryKey:				pk,
  }

  return table
}

func RowToArray(rows *sql.Rows, columns []*Column) []NullFieldData {
	buff := make([]interface{}, len(columns))
	data := make([]NullFieldData, len(columns))
	for i, _ := range buff {
    data[i].column = columns[i]
    buff[i] = &data[i]
	}
	rows.Scan(buff...)
	return data
}
