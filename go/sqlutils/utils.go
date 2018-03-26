package sqlutils

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/outbrain/golib/log"
)

func GetChunkSqlQuery(table *Table, chunkMax interface{}, offset int64) string {
	if chunkMax == nil {
		chunkMax = 0
	}
	keyForChunks := table.GetPrimaryOrUniqueKey()
	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s >= %d LIMIT 1 OFFSET %d",
		keyForChunks, table.Schema, table.Name, keyForChunks, chunkMax, offset)
	return query
}

func parseString(s interface{}) []byte {

	escape := false
	var rets []byte
	for _, b := range s.([]byte) {
		switch b {
		case byte('\''):
			escape = true
		case byte('\\'):
			escape = true
		case byte('"'):
			escape = true
		case byte('\n'):
			b = byte('n')
			escape = true
		case byte('\r'):
			b = byte('r')
			escape = true
		}

		if escape {
			rets = append(rets, byte('\\'), b)
			escape = false
		} else {
			rets = append(rets, b)
		}
	}
	return rets
}

func TablesFromDatabase(databasesParam string, db *sql.DB) map[string]bool {
	ret := make(map[string]bool)

	databases := strings.Split(databasesParam, ",")

	query := fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME "+
		"FROM information_schema.TABLES WHERE TABLE_SCHEMA IN('%s') AND TABLE_TYPE ='BASE TABLE'  AND "+
		"NOT (TABLE_SCHEMA = 'mysql' AND (TABLE_NAME = 'slow_log' OR TABLE_NAME = 'general_log'))", strings.Join(databases, "','"))

	log.Debug("Query: ", query)

	stmt, err := db.Prepare(query)

	if err != nil {
		log.Error(err.Error())
	}

	defer stmt.Close()

	rows, err := stmt.Query()

	if err != nil {
		log.Error(err.Error())
	}

	var table, schema, schematable string

	for rows.Next() {

		err = rows.Scan(&schema, &table)

		if err != nil {
			log.Error(err.Error())
		}
		schematable = fmt.Sprintf("%s.%s", schema, table)
		if _, ok := ret[schematable]; !ok {
			ret[schematable] = true
		}
	}
	return ret
}

func TablesFromString(tablesParam string) map[string]bool {
	ret := make(map[string]bool)

	tables := strings.Split(tablesParam, ",")

	for _, table := range tables {
		if _, ok := ret[table]; !ok {
			ret[table] = true
		}
	}
	return ret
}
