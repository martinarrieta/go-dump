package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/martinarrieta/go-dump/go/sqlutils"
	"github.com/martinarrieta/go-dump/go/tasks"
	"github.com/outbrain/golib/log"
)

type MySQLHost struct {
	HostName   string
	SocketFile string
	Port       int
}

type MySQLCredentials struct {
	User     string
	Password string
}

// WaitGroup for the creation of the chunks
var wgCreateChunks sync.WaitGroup

// WaitGroup to process the chunks
var wgProcessChunks sync.WaitGroup

func GetMySQLConnection(host *MySQLHost, credentials *MySQLCredentials) *sql.DB {
	var hoststring, userpass string

	userpass = fmt.Sprintf("%s:%s", credentials.User, credentials.Password)

	if len(host.SocketFile) > 0 {
		hoststring = fmt.Sprintf("unix(%s)", host.SocketFile)
	} else {
		hoststring = fmt.Sprintf("tcp(%s:%d)", host.HostName, host.Port)
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%s@%s/", userpass, hoststring))
	if err != nil {
		log.Fatalf("Panic GetMySQLConnection: %s", err.Error())
	}
	return db
}

func GetMySQLCredentials() *MySQLCredentials {
	return &MySQLCredentials{}
}

func GetMySQLHost() *MySQLHost {
	return &MySQLHost{}
}

type DumpOptions struct {
	Tables                string
	MySQLHost             *MySQLHost
	MySQLCredentials      *MySQLCredentials
	Threads               int
	ChunkSize             int64
	ChannelBufferSize     int
	MaxProcess            int
	LockTables            bool
	Debug                 bool
	TablesWithoutPKOption string
	DestinationDir        string
}

func GetDumpOptions() *DumpOptions {
	return &DumpOptions{
		MySQLHost:        GetMySQLHost(),
		MySQLCredentials: GetMySQLCredentials(),
	}
}

func main() {

	var flagTables, flagDatabases string
	dumpOptions := GetDumpOptions()

	flag.StringVar(&flagTables, "tables", "", "Tables to dump")
	flag.StringVar(&flagDatabases, "databases", "", "Databases to dump")
	flag.StringVar(&dumpOptions.MySQLHost.HostName, "host", "localhost", "MySQL hostname")
	flag.StringVar(&dumpOptions.MySQLHost.SocketFile, "socket", "", "MySQL socket file")
	flag.IntVar(&dumpOptions.MySQLHost.Port, "port", 3306, "MySQL port number")
	flag.StringVar(&dumpOptions.MySQLCredentials.User, "mysql-user", "root", "MySQL user name")
	flag.StringVar(&dumpOptions.MySQLCredentials.Password, "password", "", "MySQL password")
	flag.IntVar(&dumpOptions.Threads, "threads", 1, "Number of threads to use")
	flag.IntVar(&dumpOptions.MaxProcess, "max-process", 1, "Maximum number of process")
	flag.Int64Var(&dumpOptions.ChunkSize, "chunk-size", 1000, "Chunk size to get the rows")
	flag.IntVar(&dumpOptions.ChannelBufferSize, "channel-buffer-size", 1000, "Task channel buffer size")
	flag.BoolVar(&dumpOptions.LockTables, "lock-tables", true, "Lock tables to get consistent backup")
	flag.StringVar(&dumpOptions.TablesWithoutPKOption, "tables-without-pk", "error", "Action to have with tables without primary key [error, skip, single-chunk]")
	flag.BoolVar(&dumpOptions.Debug, "debug", false, "Lock tables to get consistent backup")
	flag.StringVar(&dumpOptions.DestinationDir, "destination", "", "Directory to store the dumps")

	flag.Parse()

	switch dumpOptions.TablesWithoutPKOption {
	case "error", "skip", "single-chunk":

	default:
		log.Fatalf("Error: \"%s\" is not a valid option for --tables-without-pk.", dumpOptions.TablesWithoutPKOption)
	}

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

	cores := runtime.NumCPU()
	if dumpOptions.Threads > cores {
		log.Warningf("The number of cores available is %d and the number of threads requested were %d."+
			"We will set the number of cores to %d.", cores, dumpOptions.Threads, cores)
		dumpOptions.Threads = cores
	}
	runtime.GOMAXPROCS(dumpOptions.Threads)

	taskManager := tasks.NewTaskManager(
		&wgCreateChunks,
		&wgProcessChunks,
		cDataChunk,
		GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials),
		dumpOptions.Threads,
		dumpOptions.DestinationDir,
		dumpOptions.TablesWithoutPKOption)

	var tablesFromDatabases, tablesFromString, tablesToParse map[string]bool

	dbtm := GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)

	if len(flagDatabases) > 0 {
		tablesFromDatabases = sqlutils.TablesFromDatabase(flagDatabases, dbtm)
		log.Debugf("tablesFromDatabases: %v ", tablesFromDatabases)
	}

	if len(flagTables) > 0 {
		tablesFromString = sqlutils.TablesFromString(flagTables)
	}

	tablesToParse = tablesFromDatabases
	for table, _ := range tablesFromString {
		if _, ok := tablesToParse[table]; !ok {
			tablesToParse[table] = true
		}
	}

	for table, _ := range tablesToParse {
		t := strings.Split(table, ".")
		taskManager.AddTask(tasks.NewTask(t[0], t[1], dumpOptions.ChunkSize, dbtm, &taskManager))
	}

	log.Debugf("Added %d connections to the taskManager", dumpOptions.Threads)

	for i := 0; i < dumpOptions.Threads; i++ {
		taskManager.AddWorkerDB(GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials))
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
