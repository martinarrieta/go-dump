package sqlutils

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/outbrain/golib/log"
)

func ParseString(s interface{}) []byte {

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

func getTablesFromQuery(query string, db *sql.DB) map[string]bool {
	ret := make(map[string]bool)

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

func TablesFromAllDatabases(db *sql.DB) map[string]bool {

	query := fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME " +
		"FROM information_schema.TABLES WHERE TABLE_TYPE ='BASE TABLE'  AND " +
		"TABLE_SCHEMA NOT IN ('performance_schema') AND " +
		"NOT (TABLE_SCHEMA = 'mysql' AND (TABLE_NAME = 'slow_log' OR TABLE_NAME = 'general_log'))")

	log.Debug("Query: ", query)
	return getTablesFromQuery(query, db)
}

func TablesFromDatabase(databasesParam string, db *sql.DB) map[string]bool {

	databases := strings.Split(databasesParam, ",")

	query := fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME "+
		"FROM information_schema.TABLES WHERE TABLE_SCHEMA IN('%s') AND TABLE_TYPE ='BASE TABLE'  AND "+
		"NOT (TABLE_SCHEMA = 'mysql' AND TABLE_NAME NOT IN ( 'slow_log' , 'general_log'))", strings.Join(databases, "','"))

	log.Debug("Query: ", query)
	return getTablesFromQuery(query, db)
}

func GetFlushTablesWithReadLockSQL() string {
	return fmt.Sprintf("FLUSH TABLES WITH READ LOCK")
}

func GetUseDatabaseSQL(schema string) string {
	return fmt.Sprintf("USE %s", schema)
}

func GetMasterStatusSQL() string {
	return fmt.Sprintf("SHOW MASTER STATUS")
}

func GetDropTableIfExistSQL(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
}

func GetShowCreateTableSQL(table string) string {
	return fmt.Sprintf("SHOW CREATE TABLE %s", table)
}

func GetShowColumnsTableSQL(table string) string {
	return fmt.Sprintf("SHOW COLUMNS FROM %s", table)
}
