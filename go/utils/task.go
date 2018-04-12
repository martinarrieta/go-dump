package utils

import (
	"database/sql"
	"fmt"

	"github.com/outbrain/golib/log"
)

type Task struct {
	Table           *Table
	ChunkSize       uint64
	OutputChunkSize uint64
	TaskManager     *TaskManager
	Tx              *sql.Tx
	DB              *sql.DB
	Id              int64
	TotalChunks     uint64
	chunkMin        int64
	chunkMax        int64
}

func (this *Task) AddChunk(chunk DataChunk) {
	this.TaskManager.AddChunk(chunk)
	this.TotalChunks = this.TotalChunks + 1
	this.TaskManager.TotalChunks = this.TaskManager.TotalChunks + 1
	this.TaskManager.Queue = this.TaskManager.Queue + 1
	this.chunkMin = this.chunkMax + 1
	log.Debugf("Queue +1: %d ", this.TaskManager.Queue)
}

func (this *Task) GetChunkSqlQuery() string {
	keyForChunks := this.Table.GetPrimaryOrUniqueKey()
	//log.Debugf("Creating chunk for table: %s")

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= %d LIMIT 1 OFFSET %d",
		keyForChunks, this.Table.GetFullName(), keyForChunks, this.chunkMax, this.ChunkSize)
	//log.Debugf("Creating chunk Query: %s", query)

	return query
}

func (this *Task) GetLastChunkSqlQuery() string {

	keyForChunks := this.Table.GetPrimaryOrUniqueKey()
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s >= %d LIMIT 1",
		keyForChunks, this.Table.GetFullName(), keyForChunks, this.chunkMin)
}

func (this *Task) CreateChunks(db *sql.DB) {
	this.TotalChunks = 0
	this.chunkMax = 0
	this.chunkMin = 0

	var (
		tx       = db
		chunkMax = int64(0)
		chunkMin = int64(0)
		stopLoop = false
	)

	defer func() {
		this.TaskManager.CreateChunksWaitGroup.Done()
	}()

	if len(this.Table.GetPrimaryOrUniqueKey()) == 0 {
		switch this.TaskManager.TablesWithoutPKOption {
		case "single-chunk":
			log.Debugf(`Table %s doesn't have any primary or unique key,
				we will make it in a single chunk.`, this.Table.GetFullName())
			this.AddChunk(NewSingleDataChunk(this))
			stopLoop = true
		case "error":
			log.Fatalf(`The table %s doesn't have any primary or unique key and the
				 --tables-without-uniquekey is \"error\"`, this.Table.GetFullName())
		}
	}

	for !stopLoop {

		err := tx.QueryRow(this.GetChunkSqlQuery()).Scan(&chunkMax)
		if err != nil && err == sql.ErrNoRows {

			err := tx.QueryRow(this.GetLastChunkSqlQuery()).Scan(&chunkMin)
			if err == nil {
				this.AddChunk(NewDataLastChunk(this))
			}
			stopLoop = true
		} else {
			this.chunkMax = chunkMax
			this.AddChunk(NewDataChunk(this))
		}
	}

	log.Debugf("Table processed %s - %d chunks created",
		this.Table.GetFullName(), this.TotalChunks)

}

func (this *Task) PrintInfo() {
	var estimatedChunks = int(0)
	chunks := float64(this.Table.estNumberOfRows) / float64(this.ChunkSize)
	if chunks > 0 {
		estimatedChunks = int(chunks + 1)
	}

	log.Infof("Table: %s Engine: %s Estimated Chunks: %v", this.Table.GetUnescapedFullName(), this.Table.Engine,
		estimatedChunks)
}

func NewTask(schema string,
	table string,
	chunkSize uint64,
	outputChunkSize uint64,
	db *sql.DB,
	tm *TaskManager) Task {
	return Task{
		Table:           NewTable(schema, table, db),
		ChunkSize:       chunkSize,
		OutputChunkSize: outputChunkSize,
		DB:              db,
		TaskManager:     tm}
}
