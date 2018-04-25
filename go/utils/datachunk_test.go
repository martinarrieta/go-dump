package utils

import "testing"

type ChunksTest struct {
	task                 *Task
	expectSingleChunkSQL string
	expectChunkSQL       string
	expectLastChunkSQL   string
}

var chunksTests = []ChunksTest{

	{task: &task1,
		expectSingleChunkSQL: "SELECT /*!40001 SQL_NO_CACHE */ * FROM `sakila`.`city`",
		expectLastChunkSQL:   "SELECT /*!40001 SQL_NO_CACHE */ * FROM `sakila`.`city` WHERE city_id >= ? ORDER BY city_id",
		expectChunkSQL:       "SELECT /*!40001 SQL_NO_CACHE */ * FROM `sakila`.`city` WHERE city_id BETWEEN ? AND ? ORDER BY city_id"},

	{task: &task2,
		expectSingleChunkSQL: "SELECT /*!40001 SQL_NO_CACHE */ * FROM `sakila`.`country`",
		expectLastChunkSQL:   "SELECT /*!40001 SQL_NO_CACHE */ * FROM `sakila`.`country` WHERE country_id >= ? ORDER BY country_id",
		expectChunkSQL:       "SELECT /*!40001 SQL_NO_CACHE */ * FROM `sakila`.`country` WHERE country_id BETWEEN ? AND ? ORDER BY country_id"},
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
