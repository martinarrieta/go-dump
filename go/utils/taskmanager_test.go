package utils

import "testing"

var taskManager = TaskManager{
	ThreadsCount:          4,
	DestinationDir:        "/tmp/out",
	TablesWithoutPKOption: "single-chunk",
	SkipUseDatabase:       true,
	GetMasterStatus:       true,
}
var table1 = &Table{
	name:       "table1",
	schema:     "schema1",
	IsLocked:   false,
	primaryKey: "pk",
}
var table2 = &Table{
	name:       "table2",
	schema:     "schema2",
	IsLocked:   false,
	primaryKey: "pk",
}
var task1 = Task{
	Table:           table1,
	ChunkSize:       1000,
	OutputChunkSize: 100,
	TaskManager:     &taskManager}
var task2 = Task{
	Table:           table1,
	ChunkSize:       1000,
	OutputChunkSize: 100,
	TaskManager:     &taskManager}

type TaskTest struct {
	table                      *Table
	chunkSize, outputChunkSize int64
	chunkMax, chunkMin         int64
	expect                     string
}

func TestTaskmanager(t *testing.T) {

	taskManager.AddTask(task1)
	taskManager.AddTask(task2)
	if len(taskManager.GetTasksPool()) != 2 {
		t.Fatal("TaskPook is not 1")
	}
}

func TestTable(t *testing.T) {
	tables := []struct {
		table             *Table
		name              string
		schema            string
		fullname          string
		unescapedfullname string
	}{
		{table1, "`table1`", "`schema1`", "`schema1`.`table1`", "schema1.table1"},
		{table2, "`table2`", "`schema2`", "`schema2`.`table2`", "schema2.table2"},
	}

	for _, tt := range tables {
		if tt.table.GetName() != tt.name {
			t.Fatalf("Table name is %s and we expect %s.", tt.table.GetName(), tt.name)
		}
		if tt.table.GetSchema() != tt.schema {
			t.Fatalf("Table schema is %s and we expect %s.", tt.table.GetSchema(), tt.schema)
		}
		if tt.table.GetFullName() != tt.fullname {
			t.Fatalf("Table fullname is %s and we expect %s.", tt.table.GetFullName(), tt.fullname)
		}
		if tt.table.GetUnescapedFullName() != tt.unescapedfullname {
			t.Fatalf("Table unescaped fullname is %s and we expect %s.", tt.table.GetUnescapedFullName(), tt.unescapedfullname)
		}
	}

}

func TestTaskGetChunkSqlQuery(t *testing.T) {

	var tablesChunk = []TaskTest{
		{table: table1,
			chunkSize: 100,
			chunkMax:  1570,
			expect:    "SELECT pk FROM `schema1`.`table1` WHERE pk >= 1570 LIMIT 1 OFFSET 100"},
		{table: table2,
			chunkSize: 1500,
			chunkMax:  0,
			expect:    "SELECT pk FROM `schema2`.`table2` WHERE pk >= 0 LIMIT 1 OFFSET 1500"},
	}
	for _, tt := range tablesChunk {
		task := Task{
			Table:           tt.table,
			ChunkSize:       tt.chunkSize,
			chunkMax:        tt.chunkMax,
			chunkMin:        tt.chunkMin,
			OutputChunkSize: tt.outputChunkSize,
			TaskManager:     &taskManager}
		query := task.GetChunkSqlQuery()
		if query != tt.expect {
			t.Errorf("Error: got \n\"%s\" instead of \n\"%s\"", query, tt.expect)
		}
	}
}
func TestTaskGetLastChunkSqlQuery(t *testing.T) {

	var tablesLastChunk = []TaskTest{

		{table: table1,
			chunkMin: 1570,
			expect:   "SELECT pk FROM `schema1`.`table1` WHERE pk >= 1570 LIMIT 1"},
		{table: table2,
			chunkMin: 100,
			expect:   "SELECT pk FROM `schema2`.`table2` WHERE pk >= 100 LIMIT 1"},
	}
	for _, tt := range tablesLastChunk {
		task := Task{
			Table:           tt.table,
			ChunkSize:       tt.chunkSize,
			chunkMax:        tt.chunkMax,
			chunkMin:        tt.chunkMin,
			OutputChunkSize: tt.outputChunkSize,
			TaskManager:     &taskManager}
		query := task.GetLastChunkSqlQuery()
		if query != tt.expect {
			t.Errorf("Error: got \n\"%s\" instead of \n\"%s\"", query, tt.expect)
		}
	}
}
