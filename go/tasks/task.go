package tasks

import (
	"database/sql"

	"github.com/martinarrieta/go-dump/go/sqlutils"
	"github.com/outbrain/golib/log"
)

type Task struct {
	Table       *sqlutils.Table
	ChunkSize   int64
	TaskManager *TaskManager
	Tx          *sql.Tx
	DB          *sql.DB
	Id          int64
	TotalChunks int64
}

func (this *Task) CreateChunks(db *sql.DB) {
	this.TaskManager.CreateChunksWaitGroup.Add(1)
	log.Debug("Added 1 to waitgroup CreateChunksWaitGroup")

	var (
		tx, _       = db.Begin()
		offset      = this.ChunkSize
		chunkMax    = int64(0)
		chunkMin    = int64(0)
		chunkNumber = int64(0)
		stopLoop    = false
	)

	defer func() {
		this.TaskManager.CreateChunksWaitGroup.Done()
		log.Debug("Passed Done with 1 to waitgroup CreateChunksWaitGroup")
	}()

	if len(this.Table.GetPrimaryOrUniqueKey()) == 0 {
		switch this.TaskManager.TablesWithoutPKOption {
		case "single-chunk":
			log.Debugf("Table %s doesn't have any primary or unique key, "+
				"we will make it in a single chunk.", this.Table.GetFullName())
			this.TaskManager.ChunksChannel <- NewSingleDataChunk(this)
			chunkNumber = 1
		case "error":
			log.Fatalf("The table %s doesn't have any primary or unique key and the"+
				" --tables-without-uniquekey is \"error\"", this.Table.GetFullName())
		}
	} else {
		log.Debugf("Processing table %s with unique key: %s",
			this.Table.GetFullName(), this.Table.GetPrimaryOrUniqueKey())

		for stopLoop == false {
			query := sqlutils.GetChunkSqlQuery(this.Table, chunkMax, offset)
			log.Debugf("Query normal chunk: %s", query)
			err := tx.QueryRow(query).Scan(&chunkMax)
			if err != nil && err == sql.ErrNoRows {
				log.Debugf("Getting last chunk for %s", this.Table.GetFullName())
				query := sqlutils.GetChunkSqlQuery(this.Table, chunkMin, 0)
				err := tx.QueryRow(query).Scan(&chunkMin)
				if err == nil {
					chunkNumber = chunkNumber + 1
					chunkMax = chunkMin + this.ChunkSize
					this.TaskManager.ChunksChannel <- NewDataLastChunk(chunkMin, chunkNumber, this)
					stopLoop = true
				} else {
					log.Infof("Done with table: %s", this.Table.GetFullName())
					stopLoop = true
				}
			} else {

				chunkNumber = chunkNumber + 1
				this.TaskManager.TotalChunks += 1
				this.TaskManager.ChunksChannel <- NewDataChunk(chunkMin, chunkMax, chunkNumber, this)
				chunkMin = chunkMax + 1
			}
		}
		/*
			if chunkNumber == 0 {
				chunkNumber = 1
				this.TaskManager.ChunksChannel <- NewDataChunk(0, offset, chunkNumber, this)
			}
		*/
	}
	this.TotalChunks = chunkNumber
	log.Infof("Table processed %s - %d chunks created",
		this.Table.GetFullName(), chunkNumber)

}

func NewTask(schema string,
	table string,
	chunkSize int64,
	db *sql.DB,
	tm *TaskManager) Task {
	return Task{
		Table:       sqlutils.NewTable(schema, table, db),
		ChunkSize:   chunkSize,
		DB:          db,
		TaskManager: tm}
}
