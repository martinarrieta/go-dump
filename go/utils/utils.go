package utils

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

	query := fmt.Sprintf(`SELECT TABLE_SCHEMA, TABLE_NAME
		FROM information_schema.TABLES WHERE TABLE_TYPE ='BASE TABLE'  AND
		TABLE_SCHEMA NOT IN ('performance_schema') AND
		NOT (TABLE_SCHEMA = 'mysql' AND (TABLE_NAME = 'slow_log' OR TABLE_NAME = 'general_log'))`)

	log.Debug("Query: ", query)
	return getTablesFromQuery(query, db)
}

func TablesFromDatabase(databasesParam string, db *sql.DB) map[string]bool {

	databases := strings.Split(databasesParam, ",")

	query := fmt.Sprintf(`SELECT TABLE_SCHEMA, TABLE_NAME
		FROM information_schema.TABLES WHERE TABLE_SCHEMA IN('%s')
		AND TABLE_TYPE ='BASE TABLE' AND NOT (TABLE_SCHEMA = 'mysql'
		AND TABLE_NAME NOT IN ('slow_log','general_log'))`, strings.Join(databases, "','"))

	log.Debug("Query: ", query)
	return getTablesFromQuery(query, db)
}

func GetFlushTablesWithReadLockSQL() string {
	return fmt.Sprintf("FLUSH TABLES WITH READ LOCK")
}

func GetLockTablesSQL(tasksPool []*Task, mode string) string {
	var tables []string
	for _, task := range tasksPool {
		tables = append(tables, fmt.Sprintf(" %s %s", task.Table.GetFullName(), mode))
	}
	return fmt.Sprintf("LOCK TABLES %s", strings.Join(tables, ","))
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

/*
func GetShowColumnsTableSQL(table *Table) string {
	return fmt.Sprintf(`SELECT COLUMN_NAME,COLUMN_KEY
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'
		  AND COLUMN_KEY IN ('PRI','UNI','MUL')
			AND DATA_TYPE IN ('tinyint','smallint','int','mediumint','bigint','timestamp')
			`, table.GetUnescapedSchema(), table.GetUnescapedName())
}
*/
type MySQLHost struct {
	HostName   string
	SocketFile string
	Port       int
}

type MySQLCredentials struct {
	User     string
	Password string
}

// GetMySQLConnection return the string to connect to the mysql server
func GetMySQLConnection(host *MySQLHost, credentials *MySQLCredentials) (*sql.DB, error) {
	var hoststring, userpass string
	userpass = fmt.Sprintf("%s:%s", credentials.User, credentials.Password)

	if len(host.SocketFile) > 0 {
		hoststring = fmt.Sprintf("unix(%s)", host.SocketFile)
	} else {
		hoststring = fmt.Sprintf("tcp(%s:%d)", host.HostName, host.Port)
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%s@%s/", userpass, hoststring))
	err = db.Ping()
	if err != nil {
		log.Fatal("MySQL connection error")
	}

	return db, nil
}
