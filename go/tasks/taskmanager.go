package tasks

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/martinarrieta/go-dump/go/sqlutils"
	"github.com/outbrain/golib/log"
)

func NewTaskManager(
	wgC *sync.WaitGroup,
	wgP *sync.WaitGroup,
	cDC chan DataChunk,
	db *sql.DB,
	threads int,
	dest string,
	tablesWithoutPKOption string) TaskManager {
	tm := TaskManager{
		CreateChunksWaitGroup:  wgC,
		ProcessChunksWaitGroup: wgP,
		ChunksChannel:          cDC,
		DB:                     db,
		ThreadsCount:           threads,
		DestinationDir:         dest,
		TablesWithoutPKOption:  tablesWithoutPKOption}
	return tm
}

type TaskManager struct {
	CreateChunksWaitGroup  *sync.WaitGroup //Create Chunks WaitGroup
	ProcessChunksWaitGroup *sync.WaitGroup //Create Chunks WaitGroup
	ChunksChannel          chan DataChunk
	DB                     *sql.DB
	ThreadsCount           int
	tasksPool              []*Task
	WorkersTx              []*sql.Tx
	WorkersDB              []*sql.DB
	TotalChunks            int64
	DestinationDir         string
	TablesWithoutPKOption  string
}

func (this *TaskManager) AddTask(t Task) {
	if len(this.tasksPool) == 0 {
		t.Id = 0
	} else {
		t.Id = this.tasksPool[len(this.tasksPool)-1].Id + 1
	}
	this.tasksPool = append(this.tasksPool, &t)
}

func (this *TaskManager) GetTasksPool() []*Task {
	return this.tasksPool
}

func (this *TaskManager) AddWorkerDB(db *sql.DB) {
	this.WorkersDB = append(this.WorkersDB, db)
	this.WorkersTx = append(this.WorkersTx, nil)
}

func (this *TaskManager) lockTable(task *Task) error {
	_, err := this.DB.Exec(fmt.Sprintf("LOCK TABLE `%s`.`%s` READ", task.Table.Schema, task.Table.Name))
	if err != nil {
		log.Errore(err)
		return err
	}
	log.Infof("Locking table: `%s`.`%s`", task.Table.Schema, task.Table.Name)
	return nil
}

func (this *TaskManager) GetTransactions(lockTables bool) {

	db := this.DB

	for _, task := range this.tasksPool {
		if err := this.lockTable(task); err != nil {
			log.Fatalf("Error locking table: %s", err.Error())
		}
	}
	log.Debug("Starting workers")
	for i, dbW := range this.WorkersDB {
		log.Debugf("Starting worker %d", i)
		if this.WorkersTx[i] == nil {
			txW, _ := dbW.Begin()
			//stm, _ := txW.Prepare("SELECT id FROM panel_socialtools_dev.campaign_collectedmessagecampaignword LIMIT 1")
			//_ = stm.QueryRow().Scan()
			this.WorkersTx[i] = txW
		}
	}
	log.Debugf("Added %d transactions", len(this.WorkersDB))

	log.Debugf("Unlocking tables")
	db.Exec("UNLOCK TABLES")
}

func (this *TaskManager) StartWorkers() error {
	log.Infof("Starting %d workers", len(this.WorkersTx))
	for i, _ := range this.WorkersTx {
		this.ProcessChunksWaitGroup.Add(1)
		go this.StartWorker(i)
	}
	log.Debugf("All workers are running")
	return nil
}

func (this *TaskManager) PrintStatus() {

	time.Sleep(2 * time.Second)
	queueSize := len(this.ChunksChannel)

	log.Infof("Printing status. Queue: %d", this.TotalChunks)

	for queueSize > 0 {
		log.Infof("Pending %d of %d", queueSize, this.TotalChunks)
		time.Sleep(1 * time.Second)
		queueSize = len(this.ChunksChannel)
	}
}

func (this *TaskManager) StartWorker(workerId int) {
	fileDescriptors := make(map[string]*os.File)
	var query string
	var stmt *sql.Stmt
	var err error
	var tablename string
	for {
		chunk, ok := <-this.ChunksChannel
		if !ok {
			log.Infof("Channel %d is closed.", workerId)
			break
		}

		if query != chunk.GetPrepareSQL() {
			query := chunk.GetPrepareSQL()
			stmt, err = this.WorkersTx[workerId].Prepare(query)
		}

		if err != nil {
			log.Fatal("Error preparring query: \"%s\" with parameters: min: %d max:%s . \nError: %s",
				query, chunk.Min, chunk.Max, err.Error())
		}
		var rows *sql.Rows
		var err error
		if chunk.IsSingleChunk == true {
			rows, err = stmt.Query()
		} else {
			rows, err = stmt.Query(chunk.Min, chunk.Max)
		}

		if err != nil {
			log.Fatal("Error executing query: \"%s\" with parameters: min: %d max:%s . \nError: %s",
				query, chunk.Min, chunk.Max, err.Error())
		}

		tablename = chunk.Task.Table.GetFullName()

		if _, ok := fileDescriptors[tablename]; !ok {
			filename := fmt.Sprintf("%s/%s-%d.sql", this.DestinationDir, tablename, workerId)
			fileDescriptors[tablename], _ = os.Create(filename)
		}

		r := sqlutils.NewRowsParser(rows, chunk.Task.Table)

		if chunk.IsSingleChunk == true {
			fmt.Fprintln(fileDescriptors[tablename], fmt.Sprintf("-- Single chunk on %s\n", tablename))
		} else {
			fmt.Fprintln(fileDescriptors[tablename], fmt.Sprintf("-- Chunk %d - from %d to %d", chunk.Sequence, chunk.Min, chunk.Max))
		}

		r.Parse(fileDescriptors[tablename])
		stmt.Close()
	}
	this.WorkersTx[workerId].Commit()
	this.ProcessChunksWaitGroup.Done()
}

func (this *TaskManager) CreateChunks() {
	for _, t := range this.tasksPool {
		go t.CreateChunks(this.DB)
	}
}

type Task struct {
	Table       *sqlutils.Table
	ChunkSize   int64
	TaskManager *TaskManager
	Tx          *sql.Tx
	DB          *sql.DB
	Id          int64
}

func (this *Task) CreateChunks(db *sql.DB) {
	log.Debug("Adding 1 to waitgroup CreateChunksWaitGroup")
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
		log.Debug("Done with 1 to waitgroup CreateChunksWaitGroup")
		this.TaskManager.CreateChunksWaitGroup.Done()
		log.Debug("Passed Done with 1 to waitgroup CreateChunksWaitGroup")
	}()

	if len(this.Table.PrimaryKey) == 0 {
		switch this.TaskManager.TablesWithoutPKOption {
		case "single-chunk":
			log.Debugf("Table %s doesn't have a primary key, we will make it in a single chunk.", this.Table.GetFullName())
			this.TaskManager.ChunksChannel <- NewSingleDataChunk(this)
			chunkNumber = 1
		case "error":
			log.Fatalf("The table %s doesn't have a primary key and the --tables-without-pk is \"error\"", this.Table.GetFullName())
		}
	} else {
		for stopLoop == false {
			query := sqlutils.GetChunkSqlQuery(this.Table, chunkMax, offset)
			log.Debugf("Query: %s", query)
			err := tx.QueryRow(query).Scan(&chunkMax)
			if err != nil {
				if err == sql.ErrNoRows {
					err := tx.QueryRow(query).Scan(&chunkMin)
					if err == nil {
						chunkNumber = chunkNumber + 1
						chunkMax = chunkMin + this.ChunkSize
						this.TaskManager.ChunksChannel <- NewDataChunk(chunkMin, chunkMax, chunkNumber, this)
					}
					stopLoop = true
				} else {
					log.Fatalf("Panic GenerateChunks: %s", err.Error())
				}
			} else {
				chunkNumber = chunkNumber + 1
				this.TaskManager.TotalChunks += 1
				this.TaskManager.ChunksChannel <- NewDataChunk(chunkMin, chunkMax, chunkNumber, this)
				chunkMin = chunkMax + 1
			}
		}
		if chunkNumber == 0 {
			this.TaskManager.ChunksChannel <- NewDataChunk(0, offset, chunkNumber, this)
		}
	}
	log.Infof("Table processed %s - %d chunks created", this.Table.GetFullName(), chunkNumber)
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

/// DataChunk

type DataChunk struct {
	Min           int64
	Max           int64
	Sequence      int64
	Task          *Task
	IsSingleChunk bool
}

func (c DataChunk) GetWhereSQL() string {
	return fmt.Sprintf("%s BETWEEN ? AND ?", c.Task.Table.PrimaryKey)
}

func (c DataChunk) GetPrepareSQL() string {
	if c.IsSingleChunk == true {
		return fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ * FROM %s.%s",
			c.Task.Table.Schema, c.Task.Table.Name)
	} else {
		return fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ * FROM %s.%s WHERE %s",
			c.Task.Table.Schema, c.Task.Table.Name, c.GetWhereSQL())
	}
}

func (c DataChunk) GetSampleSQL() string {
	return fmt.Sprintf("SELECT * FROM %s.%s LIMIT 1", c.Task.Table.Schema, c.Task.Table.Name)
}

func NewSingleDataChunk(task *Task) DataChunk {
	return DataChunk{
		Sequence:      1,
		Task:          task,
		IsSingleChunk: true}

}

func NewDataChunk(chunkMin int64, chunkMax int64, chunkNumber int64, task *Task) DataChunk {
	return DataChunk{
		Min:           chunkMin,
		Max:           chunkMax,
		Sequence:      chunkNumber,
		Task:          task,
		IsSingleChunk: false}

}
