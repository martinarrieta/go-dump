package sqlutils

import (
	"fmt"
	"log"

	"database/sql"
)

type ColumnsMap map[string]int

type Table struct {
	Name            string
	Schema          string
	primaryKey      string
	uniqueKey       string
	ColumnTypes     []*sql.ColumnType
	ColumnsOrdinals ColumnsMap
}

func (this *Table) AddColumn(column *sql.ColumnType) {
	this.ColumnTypes = append(this.ColumnTypes, column)
}

func (this *Table) GetFullName() string {
	return fmt.Sprintf("%s.%s", this.Schema, this.Name)
}

func (this *Table) GetEscapedFullName() string {
	return fmt.Sprintf("`%s`.`%s`", this.Schema, this.Name)
}

func (this *Table) GetColumn(field string) *sql.ColumnType {
	return this.ColumnTypes[this.ColumnsOrdinals[field]]
}

func (this *Table) GetPrimaryOrUniqueKey() string {
	if len(this.primaryKey) > 0 {
		return this.primaryKey
	}

	if len(this.uniqueKey) > 0 {
		return this.uniqueKey
	}

	return ""
}

func (this *Table) getTableData(db *sql.DB) error {

	rows, err := db.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", this.GetFullName()))

	if err != nil && err != sql.ErrNoRows {
		log.Fatal("Error getting column details for table ", this.GetFullName(), " : ", err.Error())
	}

	var fName, fType, fNull, fKey, fDefault, fExtra string

	for rows.Next() {
		rows.Scan(&fName, &fType, &fNull, &fKey, &fDefault, &fExtra)
		if fKey == "PRI" {
			this.primaryKey = fName
		}
		if fKey == "UNIQUE" {
			this.uniqueKey = fName
		}
	}
	return nil
}

func NewTable(schema string, name string, db *sql.DB) *Table {

	table := &Table{
		Name:   name,
		Schema: schema,
	}
	table.getTableData(db)
	return table
}
