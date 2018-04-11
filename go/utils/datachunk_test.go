package utils

import "testing"

type ChunksTest struct {
	task                       *Task
	chunkSize, outputChunkSize int64
	chunkMax, chunkMin         int64
	expectSingleChunkSQL       string
}

var chunksTests = []ChunksTest{

	{task: &task1,
		expectSingleChunkSQL: "SELECT /*!40001 SQL_NO_CACHE */ * FROM `schema1`.`table1`  ORDER BY pk"},
	{task: &task2,
		expectSingleChunkSQL: "SELECT /*!40001 SQL_NO_CACHE */ * FROM `schema2`.`table2`  ORDER BY pk"}}

func TestNewSingleDataChunk(t *testing.T) {
	for _, chunkTest := range chunksTests {
		chunk := NewSingleDataChunk(chunkTest.task)
		if chunk.GetPrepareSQL() != chunkTest.expectSingleChunkSQL {
			t.Fatalf("Got \"%s\" and expected \"%s\"", chunk.GetPrepareSQL(), chunkTest.expectSingleChunkSQL)
		}
	}
}
