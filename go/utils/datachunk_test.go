package utils

import "testing"

type ChunksTest struct {
	task                       *Task
	chunkSize, outputChunkSize int64
	chunkMax, chunkMin         int64
	expectSingleChunkSQL       string
	expectChunkSQL             string
	expectLastChunkSQL         string
}

var chunksTests = []ChunksTest{

	{task: &task1,
		expectSingleChunkSQL: "SELECT /*!40001 SQL_NO_CACHE */ * FROM `schema1`.`table1`  ORDER BY pk",
		expectLastChunkSQL:   "SELECT /*!40001 SQL_NO_CACHE */ * FROM `schema1`.`table1` WHERE pk >= ? ORDER BY pk",
		expectChunkSQL:       "SELECT /*!40001 SQL_NO_CACHE */ * FROM `schema1`.`table1` WHERE pk BETWEEN ? AND ? ORDER BY pk"},

	{task: &task2,
		expectSingleChunkSQL: "SELECT /*!40001 SQL_NO_CACHE */ * FROM `schema2`.`table2`  ORDER BY pk",
		expectLastChunkSQL:   "SELECT /*!40001 SQL_NO_CACHE */ * FROM `schema2`.`table2` WHERE pk >= ? ORDER BY pk",
		expectChunkSQL:       "SELECT /*!40001 SQL_NO_CACHE */ * FROM `schema2`.`table2` WHERE pk BETWEEN ? AND ? ORDER BY pk"},
}

func TestNewSingleDataChunk(t *testing.T) {
	for _, chunkTest := range chunksTests {
		chunk := NewSingleDataChunk(chunkTest.task)
		if chunk.GetPrepareSQL() != chunkTest.expectSingleChunkSQL {
			t.Fatalf("Got \"%s\" and expected \"%s\"", chunk.GetPrepareSQL(), chunkTest.expectSingleChunkSQL)
		}
	}
}

func TestNewDataChunk(t *testing.T) {
	for _, chunkTest := range chunksTests {
		chunk := NewDataChunk(chunkTest.task)
		if chunk.GetPrepareSQL() != chunkTest.expectChunkSQL {
			t.Fatalf("Got \"%s\" and expected \"%s\"", chunk.GetPrepareSQL(), chunkTest.expectSingleChunkSQL)
		}
	}
}

func TestNewLastDataChunk(t *testing.T) {
	for _, chunkTest := range chunksTests {
		chunk := NewDataLastChunk(chunkTest.task)
		if chunk.GetPrepareSQL() != chunkTest.expectLastChunkSQL {
			t.Fatalf("Got \"%s\" and expected \"%s\"", chunk.GetPrepareSQL(), chunkTest.expectLastChunkSQL)
		}
	}
}
