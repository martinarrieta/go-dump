package utils

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/outbrain/golib/log"
)

func NewTaskManager(
	wgC *sync.WaitGroup,
	wgP *sync.WaitGroup,
	cDC chan DataChunk,
	db *sql.DB,
	threads int,
	dest string,
	tablesWithoutPKOption string,
	skipUseDatabase bool,
	getMasterStatus bool) TaskManager {
	tm := TaskManager{
		CreateChunksWaitGroup:  wgC,
		ProcessChunksWaitGroup: wgP,
		ChunksChannel:          cDC,
		DB:                     db,
		ThreadsCount:           threads,
		DestinationDir:         dest,
		TablesWithoutPKOption:  tablesWithoutPKOption,
		SkipUseDatabase:        skipUseDatabase,
		GetMasterStatus:        getMasterStatus,
	}
	return tm
}

type TaskManager struct {
	CreateChunksWaitGroup  *sync.WaitGroup //Create Chunks WaitGroup
	ProcessChunksWaitGroup *sync.WaitGroup //Create Chunks WaitGroup
	ChunksChannel          chan DataChunk
	DB                     *sql.DB
	ThreadsCount           int
	tasksPool              []*Task
	workersTx              []*sql.Tx
	workersDB              []*sql.DB
	TotalChunks            int64
	Queue                  int64
	DestinationDir         string
	TablesWithoutPKOption  string
	extraData              map[string]interface{}
	SkipUseDatabase        bool
	GetMasterStatus        bool
}

func (this *TaskManager) setExtraData(key string, value interface{}) {
	if this.extraData == nil {
		this.extraData = make(map[string]interface{})
	}
	this.extraData[key] = value
}

func (this *TaskManager) GetExtraData(key string) interface{} {
	return this.extraData[key]
}

func (this *TaskManager) AddTask(t *Task) {
	if len(this.tasksPool) == 0 {
		t.Id = 0
	} else {
		t.Id = this.tasksPool[len(this.tasksPool)-1].Id + 1
	}
	this.tasksPool = append(this.tasksPool, t)
}

func (this *TaskManager) GetTasksPool() []*Task {
	return this.tasksPool
}

func (this *TaskManager) AddWorkerDB(db *sql.DB) {
	this.workersDB = append(this.workersDB, db)
	this.workersTx = append(this.workersTx, nil)
}

func (this *TaskManager) lockTables() {
	for _, task := range this.tasksPool {
		if err := task.Table.Lock(this.DB); err != nil {
			log.Fatalf("Error locking table: %s", err.Error())
		}
	}
}

func (this *TaskManager) unlockTables() {
	log.Debugf("Unlocking tables")
	if _, err := this.DB.Exec("UNLOCK TABLES"); err != nil {
		log.Criticalf("Error unlocking the tables: %s", err.Error())
	}
}

func (this *TaskManager) lockAllTables() {
	query := GetFlushTablesWithReadLockSQL()
	if _, err := this.DB.Exec(query); err != nil {
		log.Fatalf("Error locking table: %s", err.Error())
	}
}

func (this *TaskManager) createWorkers() {
	for i, dbW := range this.workersDB {
		//log.Infof("Starting worker %d", i)
		if this.workersTx[i] == nil {
			txW, _ := dbW.Begin()
			txW.Exec("SELECT 1")
			this.workersTx[i] = txW
		}
	}
}

func (this *TaskManager) getReplicationData() {

	log.Info("Getting Master Status")

	var masterFile, binlogDoDb, binlogIgnoreDB, executedGTIDSet string
	var masterPosition int

	masterRows, err := this.DB.Query(GetMasterStatusSQL())
	if err != nil {
		log.Fatalf("%s", err.Error())
	}
	cols, err := masterRows.Columns()
	log.Infof("Cols %+v", cols)

	if err != nil {
		log.Fatalf("%s", err.Error())
	}
	if len(cols) == 5 {
		out := []interface{}{
			&masterFile,
			&masterPosition,
			&binlogDoDb,
			&binlogIgnoreDB,
			&executedGTIDSet,
		}
		masterRows.Next()
		err := masterRows.Scan(out...)
		if err != nil {
			log.Fatalf("%s", err.Error())
		}
		masterRows.Close()
		filename := fmt.Sprintf("%s/master-data.sql", this.DestinationDir)
		log.Infof("Master File: %s\nMaster Position: %d", masterFile, masterPosition)
		file, _ := os.Create(filename)
		buffer := bufio.NewWriter(file)
		buffer.WriteString(fmt.Sprintf("Master File: %s\nMaster Position: %d\n", masterFile, masterPosition))
		buffer.Flush()
	}
}

func (this *TaskManager) WriteTablesSQL(addDropTable bool) {
	for _, task := range this.tasksPool {
		filename := fmt.Sprintf("%s/%s-definition.sql", this.DestinationDir, task.Table.GetUnescapedFullName())
		file, _ := os.Create(filename)
		buffer := bufio.NewWriter(file)
		if this.SkipUseDatabase == false {
			buffer.WriteString(GetUseDatabaseSQL(task.Table.GetSchema()) + ";\n")
		}

		buffer.WriteString("/*!40101 SET NAMES binary*/;\n")
		buffer.WriteString("/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n")

		if addDropTable {
			buffer.WriteString(GetDropTableIfExistSQL(task.Table.GetName()) + ";\n")
		}

		buffer.WriteString(task.Table.GetExtra("tableSQL").(string) + ";\n")

		buffer.Flush()
	}
}

func (this *TaskManager) GetTransactions(lockTables bool, allDatabases bool) {

	var startLocking time.Time

	if lockTables == true {
		log.Infof("Locking tables to get a consistent backup.")
		startLocking = time.Now()
		if allDatabases == true {
			this.lockAllTables()
		} else {
			this.lockTables()
		}
	}

	log.Debug("Starting workers")
	this.createWorkers()

	// GET MASTER DATA
	if this.GetMasterStatus == true {
		this.getReplicationData()
	}
	log.Debugf("Added %d transactions", len(this.workersDB))

	if lockTables == true {
		this.unlockTables()
		lockedTime := time.Since(startLocking)
		log.Infof("Unlocking the tables. Tables were locked for %s", lockedTime)
	}

}

func (this *TaskManager) StartWorkers() error {
	log.Infof("Starting %d workers", len(this.workersTx))
	for i, _ := range this.workersTx {
		this.ProcessChunksWaitGroup.Add(1)
		go this.StartWorker(i)
	}
	log.Debugf("All workers are running")
	return nil
}

func (this *TaskManager) DisplaySummary() error {
	for _, task := range this.tasksPool {
		fmt.Printf("   %d -> %s\n", task.TotalChunks, task.Table.GetFullName())
	}
	return nil
}

func (this *TaskManager) PrintStatus() {
	time.Sleep(2 * time.Second)
	log.Infof("Status. Queue: %d of %d", this.Queue, this.TotalChunks)
	for this.Queue > 0 {
		log.Infof("Queue: %d of %d", this.Queue, this.TotalChunks)
		time.Sleep(1 * time.Second)
	}
}

func (this *TaskManager) CleanChunkChannel() {
	for {
		_, ok := <-this.ChunksChannel
		if !ok {
			log.Debugf("Channel closed.")
			break
		}
	}
}
func (this *TaskManager) StartWorker(workerId int) {
	fileDescriptors := make(map[string]*os.File)
	var query string
	var stmt *sql.Stmt
	var err error
	for {
		chunk, ok := <-this.ChunksChannel
		this.Queue = this.Queue - 1
		log.Debugf("Queue -1: %d ", this.Queue)

		if !ok {
			log.Debugf("Channel %d is closed.", workerId)
			break
		}

		if query != chunk.GetPrepareSQL() {
			query = chunk.GetPrepareSQL()
			if stmt != nil {
				stmt.Close()
			}
			stmt, err = this.workersTx[workerId].Prepare(query)
		} else {
			stmt, err = this.workersTx[workerId].Prepare(query)
		}

		if err != nil {
			log.Fatalf("%s", err.Error())
		}

		tablename := chunk.Task.Table.GetUnescapedFullName()
		if _, ok := fileDescriptors[tablename]; !ok {
			filename := fmt.Sprintf("%s/%s-thread%d.sql", this.DestinationDir, tablename, workerId)
			fileDescriptors[tablename], _ = os.Create(filename)
		}

		chunk.Parse(stmt, fileDescriptors[tablename])
		stmt.Close()
	}
	this.workersTx[workerId].Commit()
	this.ProcessChunksWaitGroup.Done()
}

func (this *TaskManager) AddChunk(chunk DataChunk) {
	this.ChunksChannel <- chunk

}

func (this *TaskManager) CreateChunks(db *sql.DB) {
	log.Debugf("tasksPool  %v", this.tasksPool)
	for _, t := range this.tasksPool {
		this.CreateChunksWaitGroup.Add(1)
		log.Debugf("CreateChunksWaitGroup TaskManager Add %v", this.CreateChunksWaitGroup)
		go t.CreateChunks(db)
	}
	this.CreateChunksWaitGroup.Done()
	log.Debugf("CreateChunksWaitGroup TaskManager Done %v", this.CreateChunksWaitGroup)

}
