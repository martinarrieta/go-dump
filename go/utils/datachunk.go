package utils

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/outbrain/golib/log"
)

// DataChunk is the structure to handle the information of each chunk
type DataChunk struct {
	Min           int64
	Max           int64
	Sequence      uint64
	Task          *Task
	IsSingleChunk bool
	IsLastChunk   bool
}

// GetWhereSQL return the where condition for a chunk
func (this *DataChunk) GetWhereSQL() string {
	if this.IsSingleChunk {
		return ""
	}

	if this.IsLastChunk {
		return fmt.Sprintf(" WHERE %s >= ?", this.Task.Table.GetPrimaryOrUniqueKey())
	} else {
		return fmt.Sprintf(" WHERE %s BETWEEN ? AND ?", this.Task.Table.GetPrimaryOrUniqueKey())
	}
}

func (this *DataChunk) GetOrderBYSQL() string {
	if this.IsSingleChunk {
		return ""
	}

	return fmt.Sprintf(" ORDER BY %s", this.Task.Table.GetPrimaryOrUniqueKey())
}

func (this *DataChunk) GetPrepareSQL() string {

	return fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ * FROM %s%s%s",
		this.Task.Table.GetFullName(), this.GetWhereSQL(), this.GetOrderBYSQL())

}

func (this *DataChunk) GetSampleSQL() string {
	return fmt.Sprintf("SELECT * FROM %s LIMIT 1", this.Task.Table.GetFullName())
}

func (this *DataChunk) Parse(stmt *sql.Stmt, file *os.File) error {

	var rows *sql.Rows
	var err error
	if this.IsSingleChunk {
		log.Debugf("Is single chunk %s.", this.Task.Table.GetFullName())
		rows, err = stmt.Query()
	} else {
		if this.IsLastChunk {
			rows, err = stmt.Query(this.Min)
			log.Debugf("Last chunk %s.", this.Task.Table.GetFullName())
		} else {
			rows, err = stmt.Query(this.Min, this.Max)
		}
	}

	if err != nil {
		log.Fatalf("%s", err.Error())
	}

	tablename := this.Task.Table.GetFullName()

	//r := sqlutils.NewRowsParser(rows, this.Task.Table)
	buffer := bufio.NewWriter(file)

	if this.IsSingleChunk {
		buffer.WriteString(fmt.Sprintf("-- Single chunk on %s\n", tablename))
	} else {
		buffer.WriteString(fmt.Sprintf("-- Chunk %d - from %d to %d\n",
			this.Sequence, this.Min, this.Max))
	}
	if !this.Task.TaskManager.SkipUseDatabase {
		buffer.WriteString(fmt.Sprintf("USE %s\n", this.Task.Table.GetSchema()))
	}

	buffer.WriteString("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n")
	buffer.WriteString("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;\n")
	buffer.WriteString("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;\n")
	buffer.WriteString("/*!40101 SET NAMES utf8 */;\n")
	buffer.WriteString("/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;\n")
	buffer.WriteString("/*!40103 SET TIME_ZONE='+00:00' */;\n")
	buffer.WriteString("/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;\n")
	buffer.WriteString("/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;\n")
	buffer.WriteString("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;\n")
	buffer.WriteString("/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;\n")

	columns, _ := rows.ColumnTypes()
	buff := make([]interface{}, len(columns))
	data := make([]interface{}, len(columns))
	for i, _ := range buff {
		buff[i] = &data[i]
	}
	firstRow := true

	var rowsNumber = uint64(0)
	for rows.Next() {

		if rowsNumber > 0 && rowsNumber%this.Task.OutputChunkSize == 0 {
			buffer.WriteString(");\n\n")
			firstRow = true
		}

		if firstRow {
			buffer.WriteString(fmt.Sprintf("INSERT INTO %s VALUES \n(", this.Task.Table.GetName()))
		}
		err = rows.Scan(buff...)

		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		if err != nil {
			fmt.Println("error:", err)
		}
		if !firstRow {
			buffer.WriteString("),\n(")
		} else {
			firstRow = false
		}

		max := len(data)
		for i, d := range data {

			switch d.(type) {
			case []byte:
				buffer.Write([]byte("'"))
				buffer.Write(ParseString(d))
				buffer.Write([]byte("'"))
			case int64:
				buffer.WriteString(strconv.FormatInt(d.(int64), 10))
			case nil:
				buffer.Write([]byte("NULL"))
			case time.Time:
				buffer.WriteString(d.(time.Time).Format("2009-09-08 03:05:30.000000"))
			default:
				buffer.Write(d.([]byte))
			}
			if i != max-1 {
				buffer.Write([]byte(","))
			}
		}
	}
	rows.Close()
	buffer.WriteString(");\n")
	buffer.Flush()
	return nil
}

// Create a single chunk for a table, this is only when the table doesn't have
// primary key and the flag --table-without-pk-option is "single-chunk"
func NewSingleDataChunk(task *Task) DataChunk {
	return DataChunk{
		Sequence:      1,
		Task:          task,
		IsSingleChunk: true}

}

func NewDataChunk(task *Task) DataChunk {

	return DataChunk{
		Min:           task.chunkMin,
		Max:           task.chunkMax,
		Sequence:      task.TotalChunks,
		Task:          task,
		IsSingleChunk: false,
		IsLastChunk:   false}
}

func NewDataLastChunk(task *Task) DataChunk {

	return DataChunk{
		Min:           task.chunkMin,
		Sequence:      task.TotalChunks,
		Task:          task,
		IsSingleChunk: false,
		IsLastChunk:   true}
}
