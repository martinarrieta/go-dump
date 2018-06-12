package utils

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/outbrain/golib/log"
)

type DBBase interface {
	GetLockAllTablesSQL() string
	GetLockTablesSQL([]*Task, string) string
	GetUseDatabaseSQL(string) string
	GetMasterStatusSQL() string
	GetDropTableIfExistSQL(table string) string
	GetShowCreateTableSQL(table string) string
	GetDBConnection(host *MySQLHost, credentials *MySQLCredentials) (*sql.DB, error)
	GetTablesFromAllDatabases(db *sql.DB) map[string]bool
	GetTablesFromDatabase(databasesParam string, db *sql.DB) map[string]bool
}

type abstractDBBase struct{ DBBase }

func NewMySQLBase() *MySQLBase {
	dbbase := MySQLBase{abstractDBBase{}}
	dbbase.abstractDBBase.DBBase = dbbase
	return &dbbase
}

type MySQLBase struct{ abstractDBBase }

func (mysqlbase MySQLBase) getTablesFromQuery(query string, db *sql.DB) map[string]bool {
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

func (mysqlbase MySQLBase) GetTablesFromAllDatabases(db *sql.DB) map[string]bool {

	query := fmt.Sprintf(`SELECT TABLE_SCHEMA, TABLE_NAME
		FROM information_schema.TABLES WHERE TABLE_TYPE ='BASE TABLE'  AND
		TABLE_SCHEMA NOT IN ('performance_schema') AND
		NOT (TABLE_SCHEMA = 'mysql' AND (TABLE_NAME = 'slow_log' OR TABLE_NAME = 'general_log'))`)

	log.Debug("Query: ", query)
	return mysqlbase.getTablesFromQuery(query, db)
}

func (mysqlbase MySQLBase) GetTablesFromDatabase(databasesParam string, db *sql.DB) map[string]bool {

	databases := strings.Split(databasesParam, ",")

	query := fmt.Sprintf(`SELECT TABLE_SCHEMA, TABLE_NAME
		FROM information_schema.TABLES WHERE TABLE_SCHEMA IN('%s')
		AND TABLE_TYPE ='BASE TABLE' AND NOT (TABLE_SCHEMA = 'mysql'
		AND TABLE_NAME NOT IN ('slow_log','general_log'))`, strings.Join(databases, "','"))

	log.Debug("Query: ", query)
	return mysqlbase.getTablesFromQuery(query, db)
}

func (mysqlbase MySQLBase) GetLockAllTablesSQL() string {
	return fmt.Sprintf("FLUSH TABLES WITH READ LOCK")
}

func (mysqlbase MySQLBase) GetLockTablesSQL(tasksPool []*Task, mode string) string {
	var tables []string
	for _, task := range tasksPool {
		tables = append(tables, fmt.Sprintf(" %s %s", task.Table.GetFullName(), mode))
	}
	return fmt.Sprintf("LOCK TABLES %s", strings.Join(tables, ","))
}

func (mysqlbase MySQLBase) GetUseDatabaseSQL(schema string) string {
	return fmt.Sprintf("USE %s", schema)
}

func (mysqlbase MySQLBase) GetMasterStatusSQL() string {
	return fmt.Sprintf("SHOW MASTER STATUS")
}

func (mysqlbase MySQLBase) GetDropTableIfExistSQL(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
}

func (mysqlbase MySQLBase) GetShowCreateTableSQL(table string) string {
	return fmt.Sprintf("SHOW CREATE TABLE %s", table)
}

// GetDBConnection return the string to connect to the mysql server
func (mysqlbase MySQLBase) GetDBConnection(host *MySQLHost, credentials *MySQLCredentials) (*sql.DB, error) {
	var hoststring, userpass string
	userpass = fmt.Sprintf("%s:%s", credentials.User, credentials.Password)

	if len(host.SocketFile) > 0 {
		hoststring = fmt.Sprintf("unix(%s)", host.SocketFile)
	} else {
		hoststring = fmt.Sprintf("tcp(%s:%d)", host.HostName, host.Port)
	}
	log.Debugf(fmt.Sprintf("%s@%s/", userpass, hoststring))
	db, err := sql.Open("mysql", fmt.Sprintf("%s@%s/", userpass, hoststring))
	err = db.Ping()
	if err != nil {
		log.Fatalf("MySQL connection error: %s", err.Error())
	}

	return db, nil
}
