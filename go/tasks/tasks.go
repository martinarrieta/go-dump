// Package tasks is a package to handle all the tasks
package tasks

import (
  "fmt"
  "database/sql"
  _ "github.com/go-sql-driver/mysql"
  "github.com/martinarrieta/go-dump/go/sqlutils"
  "github.com/outbrain/golib/log"
)

type Task struct {
  Table                   *sqlutils.Table
  ChunkSize               int
  TaskManager             *TaskManager
  Tx                      *sql.Tx
  DB                      *sql.DB
  Id                      int64
}

func (this *Task) CreateChunks(db *sql.DB){
  log.Debug("Adding 1 to waitgroup CreateChunksWaitGroup")
  this.TaskManager.CreateChunksWaitGroup.Add(1)
  log.Debug("Added 1 to waitgroup CreateChunksWaitGroup")


  var (

    tx, _ = db.Begin()
    offset = this.ChunkSize
    chunkMax = 0
    chunkMin = 0
    chunkNumber = 0
    stopLoop = false
  )

  defer func() {
    log.Debug("Done with 1 to waitgroup CreateChunksWaitGroup")
    this.TaskManager.CreateChunksWaitGroup.Done()
    log.Debug("Passed Done with 1 to waitgroup CreateChunksWaitGroup")
  }()

  for stopLoop == false {

    query := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= %d LIMIT 1 OFFSET %d",
      this.Table.GetPrimaryKeyName(), this.Table.GetSchameAndTable(), this.Table.GetPrimaryKeyName(), chunkMax, offset)


    err := tx.QueryRow(query).Scan(&chunkMax)
    if err != nil {
      if err == sql.ErrNoRows {
        chunkMax = chunkMin + this.ChunkSize
        this.TaskManager.ChunksChannel <- NewDataChunk(chunkMin,chunkMax,chunkNumber,this)
        stopLoop = true
      } else {
        log.Fatalf("Panic GenerateChunks: %s", err.Error())
      }
    } else {
      chunkNumber = chunkNumber + 1
      this.TaskManager.TotalChunks += 1
      this.TaskManager.ChunksChannel <- NewDataChunk(chunkMin,chunkMax,chunkNumber,this)
      chunkMin = chunkMax + 1
    }
  }
  log.Infof("Table processed %s - %d chunks created", this.Table.GetSchameAndTable(), chunkNumber)

  if chunkNumber == 0 {
    this.TaskManager.ChunksChannel <- NewDataChunk(0,offset,chunkNumber,this)
  }
}

func NewTask(schema string,
    table string,
    chunkSize int,
    field string,
    db *sql.DB,
    tm *TaskManager) Task {
  return Task {
    Table: sqlutils.NewTable(schema,table,db),
    ChunkSize: chunkSize,
    DB: db,
    TaskManager: tm
  }
}
