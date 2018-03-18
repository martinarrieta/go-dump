package tasks

import "fmt"

type DataChunk struct {
  Min                     int
  Max                     int
  Sequence                int
  Task                    *Task
}

func (c DataChunk) GetWhereSQL() string {
  return fmt.Sprintf("%s BETWEEN ? AND ?", c.Task.Table.PrimaryKey.Name)
}

func (c DataChunk) GetPrepareSQL() string {
  return fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ * FROM %s.%s WHERE %s",
    c.Task.Table.Schema, c.Task.Table.Name, c.GetWhereSQL())
}

func (c DataChunk) GetSampleSQL() string {
  return fmt.Sprintf("SELECT * FROM %s.%s LIMIT 1", c.Task.Table.Schema, c.Task.Table.Name)
}

func NewDataChunk(chunkMin int, chunkMax int, chunkNumber int, task *Task) DataChunk {
  return DataChunk{
        Min: chunkMin,
        Max: chunkMax,
        Sequence: chunkNumber,
        Task: task}

}