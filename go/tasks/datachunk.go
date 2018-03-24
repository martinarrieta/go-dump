package tasks

import "fmt"

// DataChunk is the structure to handle the information of each chunk
type DataChunk struct {
	Min           int64
	Max           int64
	Sequence      int64
	Task          *Task
	IsSingleChunk bool
}

// GetWhereSQL return the where condition for a chunk
func (this *DataChunk) GetWhereSQL() string {
	return fmt.Sprintf("%s BETWEEN ? AND ?", this.Task.Table.PrimaryKey)
}

func (this *DataChunk) GetPrepareSQL() string {
	if this.IsSingleChunk == true {
		return fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ * FROM %s.%s",
			this.Task.Table.Schema, this.Task.Table.Name)
	} else {
		return fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ * FROM %s.%s WHERE %s",
			this.Task.Table.Schema, this.Task.Table.Name, this.GetWhereSQL())
	}
}

func (this *DataChunk) GetSampleSQL() string {
	return fmt.Sprintf("SELECT * FROM %s.%s LIMIT 1", this.Task.Table.Schema, this.Task.Table.Name)
}

// Create a single chunk for a table, this is only when the table doesn't have
// primary key and the flag --table-without-pk-option is "single-chunk"
func NewSingleDataChunk(task *Task) DataChunk {
	return DataChunk{
		Sequence:      1,
		Task:          task,
		IsSingleChunk: true}

}

func NewDataChunk(chunkMin int64, chunkMax int64, chunkNumber int64, task *Task) DataChunk {
	return DataChunk{
		Min:           chunkMin,
		Max:           chunkMax,
		Sequence:      chunkNumber,
		Task:          task,
		IsSingleChunk: false}

}
