package main

import (
	"database/sql"
	"flag"
	"os"
	"runtime"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/martinarrieta/go-dump/go/tasks"
	"github.com/outbrain/golib/log"
)

// WaitGroup for the creation of the chunks
var wgCreateChunks sync.WaitGroup

// WaitGroup to process the chunks
var wgProcessChunks sync.WaitGroup

func GetMySQLConnection() *sql.DB {
	db, err := sql.Open("mysql", "root:@/panel_socialtools_dev")
	if err != nil {
		log.Fatalf("Panic GetMySQLConnection: %s", err.Error())
	}
	return db
}

type MySQLHost struct {
	HostName   string
	SocketFile string
	Port       int
}

type MySQLCredentials struct {
	User     string
	Password string
}

func GetMySQLCredentials() *MySQLCredentials {
	return &MySQLCredentials{}
}

func GetMySQLHost() *MySQLHost {
	return &MySQLHost{}
}

type DumpOptions struct {
	Tables            string
	MySQLHost         *MySQLHost
	MySQLCredentials  *MySQLCredentials
	Threads           int
	ChunkSize         int
	ChannelBufferSize int
	MaxProcess        int
	LockTables        bool
	Debug             bool
	DestinationDir    string
}

func GetDumpOptions() *DumpOptions {
	return &DumpOptions{
		MySQLHost:        GetMySQLHost(),
		MySQLCredentials: GetMySQLCredentials(),
	}
}

func main() {

	dumpOptions := GetDumpOptions()

	flag.StringVar(&dumpOptions.Tables, "tables", "", "Tables to dump")
	flag.StringVar(&dumpOptions.MySQLHost.HostName, "host", "localhost", "MySQL hostname")
	flag.StringVar(&dumpOptions.MySQLHost.SocketFile, "socket", "", "MySQL socket file")
	flag.StringVar(&dumpOptions.MySQLCredentials.User, "mysql-user", "root", "MySQL user name")
	flag.StringVar(&dumpOptions.MySQLCredentials.Password, "password", "", "MySQL password")
	flag.IntVar(&dumpOptions.Threads, "threads", 1, "Number of threads to use")
	flag.IntVar(&dumpOptions.MaxProcess, "max-process", 1, "Maximum number of process")
	flag.Int64(&dumpOptions.ChunkSize, "chunk-size", 1000, "Chunk size to get the rows")
	flag.IntVar(&dumpOptions.ChannelBufferSize, "channel-buffer-size", 1000, "Task channel buffer size")
	flag.BoolVar(&dumpOptions.LockTables, "lock-tables", true, "Lock tables to get consistent backup")
	flag.BoolVar(&dumpOptions.Debug, "debug", false, "Lock tables to get consistent backup")
	flag.StringVar(&dumpOptions.DestinationDir, "destination", "", "Directory to store the dumps")

	flag.Parse()

	if dumpOptions.DestinationDir == "" {
		log.Fatal("--destination dir is required")
	}

	if err := os.MkdirAll(dumpOptions.DestinationDir, 0755); err != nil {
		log.Fatalf("Error creating directory: %s\n%s", dumpOptions.DestinationDir, err.Error())
	}

	cDataChunk := make(chan tasks.DataChunk, dumpOptions.ChannelBufferSize)

	if dumpOptions.Debug {
		log.SetLevel(log.DEBUG)
	} else {
		log.SetLevel(log.INFO)
	}

	runtime.GOMAXPROCS(dumpOptions.Threads)

	taskManager := tasks.NewTaskManager(
		&wgCreateChunks,
		&wgProcessChunks,
		cDataChunk,
		GetMySQLConnection(),
		dumpOptions.Threads,
		dumpOptions.DestinationDir)

	dbtm := GetMySQLConnection()

	tables := strings.Split(dumpOptions.Tables, ",")

	for _, table := range tables {
		t := strings.Split(table, ".")
		taskManager.AddTask(tasks.NewTask(t[0], t[1], dumpOptions.ChunkSize, "id", dbtm, &taskManager))
	}

	log.Debugf("Added %d connections to the taskManager", dumpOptions.Threads)

	for i := 0; i < dumpOptions.Threads; i++ {
		taskManager.AddWorkerDB(GetMySQLConnection())
	}
	go taskManager.CreateChunks()

	taskManager.GetTransactions(true)

	taskManager.StartWorkers()

	log.Info("Waiting for the creation of all the chunks.")
	wgCreateChunks.Wait()
	close(cDataChunk)
	go taskManager.PrintStatus()

	log.Info("Waiting for the execution of all the chunks.")
	wgProcessChunks.Wait()
}
