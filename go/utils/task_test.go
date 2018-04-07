package utils

import "testing"

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
		{table: table3,
			chunkSize: 500,
			chunkMax:  1000,
			expect:    "SELECT uk FROM `schema3`.`table3` WHERE uk >= 1000 LIMIT 1 OFFSET 500"},
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
		{table: table3,
			chunkMin: 500,
			expect:   "SELECT uk FROM `schema3`.`table3` WHERE uk >= 500 LIMIT 1"},
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
