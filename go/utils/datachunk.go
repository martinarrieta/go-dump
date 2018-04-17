package utils

import (
	"database/sql"
	"fmt"
	"io"
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
	buffer        io.Writer
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

func (this *DataChunk) Parse(stmt *sql.Stmt, buffer *Buffer) error {

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
	//buffer := bufio.NewWriter(file)

	if this.IsSingleChunk {
		fmt.Fprintf(buffer, "-- Single chunk on %s\n", tablename)
	} else {
		fmt.Fprintf(buffer, "-- Chunk %d - from %d to %d\n",
			this.Sequence, this.Min, this.Max)
	}

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
			fmt.Fprintf(buffer, ");\n\n")

			firstRow = true
		}

		if firstRow {
			fmt.Fprintf(buffer, "INSERT INTO %s VALUES \n(", this.Task.Table.GetName())
		}
		err = rows.Scan(buff...)

		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		if err != nil {
			fmt.Println("error:", err)
		}
		if !firstRow {
			fmt.Fprintf(buffer, "),\n(")
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
				fmt.Fprintf(buffer, "%d", d)
			case nil:
				buffer.Write([]byte("NULL"))
			case time.Time:
				fmt.Fprintf(buffer, "%s", d)
			default:
				buffer.Write(d.([]byte))
			}
			if i != max-1 {
				fmt.Fprintf(buffer, ",")
			}
		}
	}
	rows.Close()
	fmt.Fprintf(buffer, ");\n")

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
