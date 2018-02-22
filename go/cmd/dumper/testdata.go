package main

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/martinarrieta/go-dump/go/sqlutils"
	//ghsql "github.com/github/gh-ost/go/sql"
	"github.com/outbrain/golib/log"
	//"github.com/outbrain/golib/sqlutils"
	//"reflect"
	"fmt"
)

func main() {
	db, err := sql.Open("mysql", "root:@/panel_socialtools_dev")
	if err != nil {
		panic(err.Error())
	}

	defer db.Close()
	tableSchema := "panel_socialtools_dev"
	tableName := "campaign_collectedmessagecampaignword"

	table := sqlutils.NewTable(tableSchema, tableName, db)

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s.%s LIMIT 10", tableSchema, tableName))
	if err != nil {
		log.Error(err.Error())
	}
	for rows.Next() {
		data := sqlutils.RowToArray(rows, table.Columns)
		log.Debugf("%+v", data[0].Value())
		log.Debugf("%+v", data[1].Value())
		log.Debugf("%+v", data[2].Value())
		log.Debug("--------------")
	}
}
