package utils

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/outbrain/golib/log"
)

func NewTaskManager(
	wgC *sync.WaitGroup,
	wgP *sync.WaitGroup,
	cDC chan DataChunk,
	dumpOptions *DumpOptions) TaskManager {

	dbtype := NewMySQLBase()
	db, err := dbtype.GetDBConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)

	if err != nil {
		log.Critical("Error whith the database connection. %s", err.Error())
	}

	tm := TaskManager{
		CreateChunksWaitGroup:  wgC,
		ProcessChunksWaitGroup: wgP,
		ChunksChannel:          cDC,
		DB:                     db,
		dbtype:                 dbtype,
		databaseEngines:        make(map[string]*Table),
		ThreadsCount:           dumpOptions.Threads,
		DestinationDir:         dumpOptions.DestinationDir,
		TablesWithoutPKOption:  dumpOptions.TablesWithoutUKOption,
		SkipUseDatabase:        dumpOptions.SkipUseDatabase,
		GetMasterStatus:        dumpOptions.GetMasterStatus,
		GetSlaveStatus:         dumpOptions.GetSlaveStatus,
		Compress:               dumpOptions.Compress,
		CompressLevel:          dumpOptions.CompressLevel,
		IsolationLevel:         dumpOptions.IsolationLevel,
		mySQLHost:              dumpOptions.MySQLHost,
		mySQLCredentials:       dumpOptions.MySQLCredentials}
	return tm
}

type TaskManager struct {
	CreateChunksWaitGroup  *sync.WaitGroup //Create Chunks WaitGroup
	ProcessChunksWaitGroup *sync.WaitGroup //Create Chunks WaitGroup
	ChunksChannel          chan DataChunk
	DB                     *sql.DB
	dbtype                 DBBase
	ThreadsCount           int
	tasksPool              []*Task
	workersTx              []*sql.Tx
	workersDB              []*sql.DB
	databaseEngines        map[string]*Table
	TotalChunks            int64
	Queue                  int64
	DestinationDir         string
	TablesWithoutPKOption  string
	SkipUseDatabase        bool
	GetMasterStatus        bool
	GetSlaveStatus         bool
	Compress               bool
	CompressLevel          int
	IsolationLevel         sql.IsolationLevel
	mySQLHost              *MySQLHost
	mySQLCredentials       *MySQLCredentials
}

func (this *TaskManager) addDatabaseEngine(t *Table) {

	if len(this.databaseEngines) == 0 {
		this.databaseEngines = make(map[string]*Table)
	}

	if _, ok := this.databaseEngines[t.Engine]; !ok {
		this.databaseEngines[t.Engine] = t
	}
}

func (this *TaskManager) AddTask(t *Task) {
	if len(this.tasksPool) == 0 {
		t.Id = 0
	} else {
		t.Id = this.tasksPool[len(this.tasksPool)-1].Id + 1
	}
	this.tasksPool = append(this.tasksPool, t)
	this.addDatabaseEngine(t.Table)
}

func (this *TaskManager) GetTasksPool() []*Task {
	return this.tasksPool
}

func (this *TaskManager) GetDBConnection() (*sql.DB, error) {
	return this.dbtype.GetDBConnection(this.mySQLHost, this.mySQLCredentials)
}
func (taskManager *TaskManager) GetTablesFromAllDatabases() map[string]bool {
	return taskManager.dbtype.GetTablesFromAllDatabases(taskManager.DB)
}

func (taskManager *TaskManager) GetTablesFromDatabase(databaseParam string) map[string]bool {
	return taskManager.dbtype.GetTablesFromDatabase(databaseParam, taskManager.DB)
}

func (this *TaskManager) AddWorkersDB() {
	for i := 0; i < this.ThreadsCount; i++ {

		conn, err := this.dbtype.GetDBConnection(this.mySQLHost, this.mySQLCredentials)
		if err != nil {
			log.Critical("Error whith the database connection. %s", err.Error())
		}
		conn.Ping()
		this.AddWorkerDB(conn)
	}

}

func (this *TaskManager) AddWorkerDB(db *sql.DB) {
	this.workersDB = append(this.workersDB, db)
	this.workersTx = append(this.workersTx, nil)
}

func (this *TaskManager) lockTables() {
	query := this.dbtype.GetLockTablesSQL(this.tasksPool, "READ")

	if _, err := this.DB.Exec(query); err != nil {
		log.Criticalf("Error unlocking the tables: %s", err.Error())
	}
}

func (this *TaskManager) unlockTables() {
	log.Debugf("Unlocking tables")
	if _, err := this.DB.Exec("UNLOCK TABLES"); err != nil {
		log.Criticalf("Error unlocking the tables: %s", err.Error())
	}
}

func (this *TaskManager) lockAllTables() {
	query := this.dbtype.GetLockAllTablesSQL()
	if _, err := this.DB.Exec(query); err != nil {
		log.Fatalf("Error locking table: %s", err.Error())
	}
}

func (this *TaskManager) createWorkers() {
	for i, dbW := range this.workersDB {
		//log.Infof("Starting worker %d", i)
		if this.workersTx[i] == nil {
			txW, _ := dbW.BeginTx(context.Background(), &sql.TxOptions{
				Isolation: this.IsolationLevel,
				ReadOnly:  true})
			for engine, table := range this.databaseEngines {
				txW.Exec(fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", table.GetFullName()))
				log.Debugf("Selecting a table from engine: %s", engine)
			}
			this.workersTx[i] = txW
		}
	}
}

func (this *TaskManager) isMultiMaster() (bool, error) {
	_, err := this.DB.Query("SELECT @@default_master_connection")
	switch err {
	case sql.ErrNoRows:
		return false, nil
	case nil:
		return true, nil
	default:
		return false, err
	}

}

// getSlaveData collects the slave data from the node that you are taking the backup.
// It detect if the slave has multi master replication and collect and store the information for all the channels.
func (this *TaskManager) getSlaveData() {
	log.Info("Getting Slave Status")
	isMultiMaster, _ := this.isMultiMaster()
	var query string

	if isMultiMaster {
		query = "SHOW ALL SLAVES STATUS"
	} else {
		query = "SHOW SLAVE STATUS"
	}
	slaveData, err := this.DB.Query(query)

	if err != nil {
		log.Fatalf("Error getting slave information: %s", err.Error())
	}

	var connectionName, relayMasterLogFile, masterHost, executedGtidSet, gtidSlavePos string
	var execMasterLogPos, masterPort uint64
	var haveGtidSlavePos, haveExecutedGtidSet = false, false
	cols, _ := slaveData.Columns()
	var out []interface{}
	iterations := 0

	for i := 0; i < len(cols); i++ {
		switch strings.ToUpper(cols[i]) {
		case "CONNECTION_NAME":
			out = append(out, &connectionName)
		case "RELAY_MASTER_LOG_FILE":
			out = append(out, &relayMasterLogFile)
		case "MASTER_HOST":
			out = append(out, &masterHost)
		case "MASTER_PORT":
			out = append(out, &masterPort)
		case "EXECUTED_GTID_SET":
			haveExecutedGtidSet = true
			out = append(out, &executedGtidSet)
		case "GTID_SLAVE_POS":
			haveGtidSlavePos = true
			out = append(out, &gtidSlavePos)
		case "EXEC_MASTER_LOG_POS":
			out = append(out, &execMasterLogPos)
		default:
			out = append(out, new(interface{}))
		}
	}
	buffer, _ := NewSlaveDataBuffer(this)

	for slaveData.Next() {
		iterations++
		if err := slaveData.Scan(out...); err != nil {
			log.Fatalf(err.Error())
		}

		fmt.Fprintln(buffer, "Connection Name: ", connectionName)
		fmt.Fprintln(buffer, "  Relay Master Log File: ", relayMasterLogFile)
		fmt.Fprintln(buffer, "  Master Host: ", masterHost)
		fmt.Fprintln(buffer, "  Master Port: ", masterPort)
		fmt.Fprintln(buffer, "  Exec Master Log Pos: ", execMasterLogPos)
		if haveExecutedGtidSet {
			fmt.Fprintln(buffer, "  Executed GTID Set: ", executedGtidSet)
		}
		if haveGtidSlavePos {
			fmt.Fprintln(buffer, "  GTID Slave Pos: ", gtidSlavePos)
		}
	}
	buffer.Flush()
	buffer.Close()

	if iterations == 0 {
		log.Fatalf("There is no slave information. Make sure that the server is acting as a slave server.")
	}
}

func (this *TaskManager) getMasterData() {

	log.Info("Getting Master Status")

	var masterFile, binlogDoDb, binlogIgnoreDB, executedGTIDSet string
	var masterPosition int

	masterRows, err := this.DB.Query(this.dbtype.GetMasterStatusSQL())
	if err != nil {
		log.Fatalf("%s", err.Error())
	}
	cols, _ := masterRows.Columns()

	if len(cols) < 1 {
		log.Fatal("Error getting the master data information. Make sure that the logs are enabled. If you want to skip the collection of the master information please use the option --master-data=false. Use --help for more information.")
	}
	var out []interface{}
	supportGTID := false

	for i := 0; i < len(cols); i++ {
		switch strings.ToUpper(cols[i]) {
		case "FILE":
			out = append(out, &masterFile)
		case "POSITION":
			out = append(out, &masterPosition)
		case "BINLOG_DO_DB":
			out = append(out, &binlogDoDb)
		case "BINLOG_IGNORE_DB":
			out = append(out, &binlogIgnoreDB)
		case "EXECUTED_GTID_SET":
			supportGTID = true
			out = append(out, &executedGTIDSet)
		default:
			log.Warningf("Unknown option \"%s\" on the Mastet Inforamtion. Please report this bug. MASTER DATA WILL NOT BE AVAILABLE!")
		}
	}

	masterRows.Next()
	err = masterRows.Scan(out...)
	if err != nil {
		log.Fatalf("Error reading Master data information: %s", err.Error())
	}
	masterRows.Close()
	buffer, _ := NewMasterDataBuffer(this)

	fmt.Fprintln(buffer, "Master File:", masterFile)
	fmt.Fprintln(buffer, "Master Position: ", masterPosition)
	fmt.Fprintln(buffer, "Binlog Do DB: ", binlogDoDb)
	fmt.Fprintln(buffer, "Binlog Ignore DB: ", binlogIgnoreDB)
	if supportGTID {
		fmt.Fprintln(buffer, "Executed Gtid Set: ", executedGTIDSet)
	}
	buffer.Flush()
	buffer.Close()
}

func (this *TaskManager) WriteTablesSQL(addDropTable bool) {
	for _, task := range this.tasksPool {
		buffer, _ := NewTableDefinitionBuffer(task)

		if !this.SkipUseDatabase {
			fmt.Fprintf(buffer, this.dbtype.GetUseDatabaseSQL(task.Table.GetSchema())+";\n")
		}

		fmt.Fprintf(buffer, "/*!40101 SET NAMES binary*/;\n")
		fmt.Fprintf(buffer, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n")

		if addDropTable {
			fmt.Fprintf(buffer, this.dbtype.GetDropTableIfExistSQL(task.Table.GetName())+";\n")
		}

		fmt.Fprintf(buffer, task.Table.CreateTableSQL+";\n")
		buffer.Flush()
	}
}

func (this *TaskManager) GetTransactions(lockTables bool, allDatabases bool) {

	var startLocking time.Time

	if lockTables {
		log.Infof("Locking tables to get a consistent backup.")
		startLocking = time.Now()
		if allDatabases {
			this.lockAllTables()
		} else {
			this.lockTables()
		}
	}
	log.Debug("Starting workers")
	this.createWorkers()

	// GET MASTER DATA
	if this.GetMasterStatus {
		this.getMasterData()
	}
	if this.GetSlaveStatus {
		this.getSlaveData()
	}

	log.Debugf("Added %d transactions", len(this.workersDB))

	if lockTables {
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
	bufferChunk := make(map[string]*Buffer)

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
			log.Fatalf("Error starting the worker. Query: %s, Error: %s.", query, err.Error())
		}

		tablename := chunk.Task.Table.GetUnescapedFullName()

		if _, ok := bufferChunk[tablename]; !ok {
			bufferChunk[tablename], _ = NewChunkBuffer(&chunk, workerId)
		}

		buffer := bufferChunk[tablename]

		if !chunk.Task.TaskManager.SkipUseDatabase {
			fmt.Fprintf(buffer, "USE %s\n", chunk.Task.Table.GetSchema())
		}

		buffer.Flush()

		chunk.Parse(stmt, buffer)

		stmt.Close()
	}
	for _, buffer := range bufferChunk {
		buffer.Close()
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

func (this *TaskManager) GetBufferOptions() *BufferOptions {
	bufferOptions := new(BufferOptions)
	if this.Compress {
		bufferOptions.Compress = true
		bufferOptions.CompressLevel = this.CompressLevel
	}
	bufferOptions.Type = BufferTypeFile
	return bufferOptions
}
