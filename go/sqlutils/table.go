package sqlutils

import (
	"database/sql"
	"fmt"
	"log"
)

type ColumnsMap map[string]int

type Table struct {
	Name            string
	Schema          string
	PrimaryKey      *Column
	Columns         []*Column
	ColumnsOrdinals ColumnsMap
}

func (this *Table) AddColumn(column *Column) {
	this.Columns = append(this.Columns, column)
}

func (this *Table) GetSchameAndTable() string {
	return fmt.Sprintf("%s.%s", this.Schema, this.Name)
}

func (this *Table) GetChunkSqlQuery(chunkMax interface{}, offset int64) string {
	if chunkMax == nil {
		chunkMax = 0
	}
	return fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s >= %d LIMIT 1 OFFSET %d",
		this.PrimaryKey.Name, this.Schema, this.Name, this.PrimaryKey.Name, chunkMax, offset)
}

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

		column, err := ProcessColumn(*rows)
		if err != nil {
			log.Fatal("error", err.Error())
		}
		columns = append(columns, column)

		columnsOrdinals[column.Name] = column.OrdinalPosition - 1
	}

	table := &Table{
		Name:            name,
		Schema:          schema,
		Columns:         columns,
		ColumnsOrdinals: columnsOrdinals,
		PrimaryKey:      pk,
	}

	return table
}
