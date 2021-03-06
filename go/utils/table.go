package utils

import (
	"database/sql"
	"fmt"

	"github.com/outbrain/golib/log"
)

type ColumnsMap map[string]int

// Table contains the name and type of a table.
type Table struct {
	name            string
	schema          string
	primaryKey      []string
	uniqueKey       []string
	keyForChunks    string
	estNumberOfRows uint64
	estDataSize     uint64
	estIndexSize    uint64

	CreateTableSQL string
	IsLocked       bool
	Engine         string
	Collation      string
}

// getColumnsInformationSQL return the SQL statment to get the columns
// information of a table
func (this *Table) getColumnsInformationSQL() string {
	return fmt.Sprintf(`SELECT COLUMN_NAME,COLUMN_KEY
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'
		  AND COLUMN_KEY IN ('PRI','UNI','MUL')
			AND DATA_TYPE IN ('tinyint','smallint','int','mediumint','bigint','timestamp')
			`, this.GetUnescapedSchema(), this.GetUnescapedName())
}

/*
TABLE_CATALOG: def
	TABLE_SCHEMA: panel_socialtools_dev
		TABLE_NAME: twitter_collector_twitterstatusentitiesmedia
		TABLE_TYPE: BASE TABLE
				ENGINE: InnoDB
			 VERSION: 10
		ROW_FORMAT: Dynamic
		TABLE_ROWS: 57891
AVG_ROW_LENGTH: 299
	 DATA_LENGTH: 17350656
MAX_DATA_LENGTH: 0
	INDEX_LENGTH: 4227072
		 DATA_FREE: 0
AUTO_INCREMENT: NULL
	 CREATE_TIME: 2018-02-15 11:58:17
	 UPDATE_TIME: NULL
		CHECK_TIME: NULL
TABLE_COLLATION: latin1_swedish_ci
			CHECKSUM: NULL
CREATE_OPTIONS:
 TABLE_COMMENT:
*/

// GetFullName return a string with database and table name escaped.
func (this *Table) GetFullName() string {
	return fmt.Sprintf("`%s`.`%s`", this.schema, this.name)
}

// GetSchema return a string with the database name escaped.
func (this *Table) GetSchema() string {
	return fmt.Sprintf("`%s`", this.schema)
}

// GetName return a string with the table name escaped.
func (this *Table) GetName() string {
	return fmt.Sprintf("`%s`", this.name)
}

// GetUnescapedSchema return a string with the database name.
func (this *Table) GetUnescapedSchema() string {
	return fmt.Sprintf("%s", this.schema)
}

// GetUnescapedName return a string with the table name.
func (this *Table) GetUnescapedName() string {
	return fmt.Sprintf("%s", this.name)
}

// GetUnescapedFullName return a string with database and table name.
func (this *Table) GetUnescapedFullName() string {
	return fmt.Sprintf("%s.%s", this.schema, this.name)
}

// GetPrimaryOrUniqueKey return a string with the name of the unique or primary
// key filed that we will use to split the table.
// Empty string means that the table doens't have any primary or unique key to use.
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

// getTableInformation collect and store the table information
func (this *Table) getTableInformation(db *sql.DB) error {

	var tableName string
	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", this.GetFullName())).Scan(&tableName, &this.CreateTableSQL)
	if err != nil {
		log.Fatalf("Error getting show create table: %s", err.Error())
	}

	query := fmt.Sprintf(`SELECT ENGINE, TABLE_COLLATION, DATA_LENGTH, INDEX_LENGTH,
		TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='%s' AND TABLE_NAME='%s'`,
		this.GetUnescapedSchema(), this.GetUnescapedName())
	err = db.QueryRow(query).Scan(&this.Engine, &this.Collation,
		&this.estDataSize, &this.estIndexSize, &this.estNumberOfRows)
	return err
}

// getData collect the table information
func (this *Table) getData(db *sql.DB) error {

	this.getTableInformation(db)

	rows, err := db.Query(this.getColumnsInformationSQL())

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
	return nil
}

// NewTable create a new Table object.
func NewTable(schema string, name string, db *sql.DB) *Table {
	table := &Table{
		name:     name,
		schema:   schema,
		IsLocked: false,
	}

	table.getData(db)
	return table
}
