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

func NewTaskManager(wgC *sync.WaitGroup, wgP *sync.WaitGroup, cDC chan DataChunk, db *sql.DB, threads int, dest string) TaskManager {
	tm := TaskManager{CreateChunksWaitGroup: wgC,
		ProcessChunksWaitGroup: wgP, ChunksChannel: cDC, DB: db, ThreadsCount: threads, DestinationDir: dest}
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
			log.Fatalf(err.Error())
		}

	}

	for i, dbW := range this.WorkersDB {
		if this.WorkersTx[i] == nil {
			txW, _ := dbW.Begin()
			stm, _ := txW.Prepare("SELECT id FROM panel_socialtools_dev.campaign_collectedmessagecampaignword LIMIT 1")
			_ = stm.QueryRow().Scan()
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
	//f, _ := os.Create(fmt.Sprintf("/tmp/dump%d.sql",workerId))
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
			log.Fatal("Error executing query: \"%s\" with parameters: min: %d max:%s . \nError: %s",
				query, chunk.Min, chunk.Max, err.Error())
		}

		rows, err := stmt.Query(chunk.Min, chunk.Max)
		if err != nil {
			log.Fatal("Error executing query: \"%s\" with parameters: min: %d max:%s . \nError: %s",
				query, chunk.Min, chunk.Max, err.Error())
		}

		tablename = fmt.Sprintf("%s.%s", chunk.Task.Table.Schema, chunk.Task.Table.Name)

		if _, ok := fileDescriptors[tablename]; !ok {
			filename := fmt.Sprintf("%s/%s-%d.sql", this.DestinationDir, tablename, workerId)
			fileDescriptors[tablename], _ = os.Create(filename)
		}

		r := sqlutils.NewRowsParser(rows, chunk.Task.Table)
		fmt.Fprintln(fileDescriptors[tablename], fmt.Sprintf("-- Chunk %d - from %d to %d", chunk.Sequence, chunk.Min, chunk.Max))

		r.Parse(fileDescriptors[tablename])
		stmt.Close()

	}
	this.WorkersTx[workerId].Commit()
	this.ProcessChunksWaitGroup.Done()
	//f.Close()
}

func (this *TaskManager) CreateChunks() {
	for _, t := range this.tasksPool {
		go t.CreateChunks(this.DB)
	}

}
