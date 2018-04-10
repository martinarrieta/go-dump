package utils

import (
	"fmt"

	"github.com/outbrain/golib/log"

	"database/sql"
)

type ColumnsMap map[string]int

type Table struct {
	name            string
	schema          string
	primaryKey      []string
	uniqueKey       []string
	keyForChunks    string
	columnTypes     []*sql.ColumnType
	columnsOrdinals ColumnsMap
	createSQL       string
	extra           map[string]interface{}
	IsLocked        bool
}

func (this *Table) setExtra(key string, value interface{}) {
	if this.extra == nil {
		this.extra = make(map[string]interface{})
	}
	this.extra[key] = value
}

func (this *Table) GetExtra(key string) interface{} {
	return this.extra[key]
}

func (this *Table) Lock(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf("LOCK TABLE %s READ", this.GetFullName()))
	if err == nil {
		this.IsLocked = true
	}
	return err
}
func (this *Table) Unlock(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf("UNLOCK TABLES"))
	if err == nil {
		this.IsLocked = false
	}
	return err
}

func (this *Table) GetColumnsSQL() string {
	return fmt.Sprintf("SHOW COLUMNS FROM %s ", this.GetFullName())
}

func (this *Table) AddColumn(column *sql.ColumnType) {
	this.columnTypes = append(this.columnTypes, column)
}

func (this *Table) GetFullName() string {
	return fmt.Sprintf("`%s`.`%s`", this.schema, this.name)
}

func (this *Table) GetSchema() string {
	return fmt.Sprintf("`%s`", this.schema)
}

func (this *Table) GetName() string {
	return fmt.Sprintf("`%s`", this.name)
}

func (this *Table) GetUnescapedSchema() string {
	return fmt.Sprintf("%s", this.schema)
}

func (this *Table) GetUnescapedName() string {
	return fmt.Sprintf("%s", this.name)
}

func (this *Table) GetUnescapedFullName() string {
	return fmt.Sprintf("%s.%s", this.schema, this.name)
}

func (this *Table) GetColumn(field string) *sql.ColumnType {
	return this.columnTypes[this.columnsOrdinals[field]]
}

func (this *Table) GetPrimaryOrUniqueKey() string {

	if len(this.keyForChunks) > 0 {
		return this.keyForChunks
	}

	if len(this.primaryKey) == 1 {
		this.keyForChunks = this.primaryKey[0]
		return this.keyForChunks
	}

	if len(this.uniqueKey) > 0 {
		this.keyForChunks = this.uniqueKey[0]
		return this.keyForChunks
	}

	return ""
}

func (this *Table) getTableData(db *sql.DB) error {

	rows, err := db.Query(GetShowColumnsTableSQL(this))

	if err != nil && err != sql.ErrNoRows {
		log.Fatal("Error getting column details for table ", this.GetFullName(), " : ", err.Error())
	}

	var cName, cKey string

	for rows.Next() {
		rows.Scan(&cName, &cKey)
		switch cKey {
		case "PRI":
			this.primaryKey = append(this.primaryKey, cName)
		case "UNI":
			this.uniqueKey = append(this.uniqueKey, cName)
		default:

		}
	}

	var tableName, tableSQL string
	err = db.QueryRow(GetShowCreateTableSQL(this.GetFullName())).Scan(&tableName, &tableSQL)
	this.setExtra("tableSQL", tableSQL)

	return nil
}

func NewTable(schema string, name string, db *sql.DB) *Table {
	table := &Table{
		name:     name,
		schema:   schema,
		IsLocked: false,
	}
	table.getTableData(db)
	return table
}
