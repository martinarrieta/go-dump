package utils

import (
	"testing"
)

var task1 = NewTask("sakila", "city", 1000, 1000, &taskManager)
var task2 = NewTask("sakila", "country", 1000, 1000, &taskManager)
var task3 = NewTask("sakila", "store_no_pk", 1000, 1000, &taskManager)

func TestAddTask(t *testing.T) {
	taskManager.AddTask(&task1)
	taskManager.AddTask(&task2)
	taskManager.AddTask(&task3)
	tasksPool := taskManager.GetTasksPool()
	if len(tasksPool) != 3 {
		t.Fatalf("TaskPool is not 3")
	}

}

type TaskTest struct {
	table                      *Table
	chunkSize, outputChunkSize uint64
	chunkMax, chunkMin         int64
	expect                     string
}

func TestTaskGetChunkSqlQuery(t *testing.T) {

	var tablesChunk = []TaskTest{
		{table: task1.Table,
			chunkSize: 100,
			chunkMax:  1570,
			expect:    "SELECT city_id FROM `sakila`.`city` WHERE city_id >= 1570 LIMIT 1 OFFSET 100"},
		{table: task2.Table,
			chunkSize: 1500,
			chunkMax:  0,
			expect:    "SELECT country_id FROM `sakila`.`country` WHERE country_id >= 0 LIMIT 1 OFFSET 1500"},
		{table: task3.Table,
			chunkSize: 500,
			chunkMax:  1000,
			expect:    "SELECT manager_staff_id FROM `sakila`.`store_no_pk` WHERE manager_staff_id >= 1000 LIMIT 1 OFFSET 500"},
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

func TestGetLockTablesSQL(t *testing.T) {

	//taskManager.GetTransactions(true, false)

}
