package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"

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

// GetMySQLConnection return the string to connect to the mysql server
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

type DumpOptions struct {
	Tables                string
	MySQLHost             *MySQLHost
	MySQLCredentials      *MySQLCredentials
	Threads               int
	ChunkSize             int64
	ChannelBufferSize     int
	LockTables            bool
	Debug                 bool
	TablesWithoutPKOption string
	DestinationDir        string
}

func GetDumpOptions() *DumpOptions {
	return &DumpOptions{
		MySQLHost:        new(MySQLHost),
		MySQLCredentials: new(MySQLCredentials),
	}
}

func main() {

	var flagTables, flagDatabases string
	dumpOptions := GetDumpOptions()

	flag.StringVar(&flagTables, "tables", "", "List of comma separated tables to dump.\nEach table should have the database name included, for example \"mydb.mytable,mydb2.mytable2\"")
	flag.StringVar(&flagDatabases, "databases", "", "List of comma separated databases to dump")
	flag.StringVar(&dumpOptions.MySQLHost.HostName, "host", "localhost", "MySQL hostname")
	flag.StringVar(&dumpOptions.MySQLHost.SocketFile, "socket", "", "MySQL socket file")
	flag.IntVar(&dumpOptions.MySQLHost.Port, "port", 3306, "MySQL port number")
	flag.StringVar(&dumpOptions.MySQLCredentials.User, "mysql-user", "", "MySQL user name")
	flag.StringVar(&dumpOptions.MySQLCredentials.Password, "password", "", "MySQL password")
	flag.IntVar(&dumpOptions.Threads, "threads", 1, "Number of threads to use")
	flag.Int64Var(&dumpOptions.ChunkSize, "chunk-size", 1000, "Chunk size to get the rows")
	flag.IntVar(&dumpOptions.ChannelBufferSize, "channel-buffer-size", 1000, "Task channel buffer size")
	flag.BoolVar(&dumpOptions.LockTables, "lock-tables", true, "Lock tables to get consistent backup")
	flag.StringVar(&dumpOptions.TablesWithoutPKOption, "tables-without-pk", "error", "Action to have with tables without primary key.\nValid actions are: \"error\", \"skip\", \"single-chunk\".")
	flag.BoolVar(&dumpOptions.Debug, "debug", false, "Display debug information.")
	flag.StringVar(&dumpOptions.DestinationDir, "destination", "", "Directory to store the dumps")

	flag.Parse()

	//Setting debug level
	if dumpOptions.Debug {
		log.SetLevel(log.DEBUG)
	} else {
		log.SetLevel(log.INFO)
	}

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Fatalf("Killing the dumper.")

	}()

	switch dumpOptions.TablesWithoutPKOption {
	case "error", "skip", "single-chunk":
		log.Debugf("The method to use with the tables without primary key is \"%s\".", dumpOptions.TablesWithoutPKOption)
	default:
		log.Fatalf("Error: \"%s\" is not a valid option for --tables-without-pk.", dumpOptions.TablesWithoutPKOption)
		flag.Usage()
	}

	if dumpOptions.DestinationDir == "" {
		log.Fatal("--destination dir is required")
	}

	if err := os.MkdirAll(dumpOptions.DestinationDir, 0755); err != nil {
		log.Fatalf("Error creating directory: %s\n%s", dumpOptions.DestinationDir, err.Error())
	}

	// Creating the buffer for the channel
	cDataChunk := make(chan tasks.DataChunk, dumpOptions.ChannelBufferSize)

	cores := runtime.NumCPU()
	if dumpOptions.Threads > cores {
		log.Warningf("The number of cores available is %d and the number of threads requested were %d."+
			"We will set the number of cores to %d.", cores, dumpOptions.Threads, cores)
		dumpOptions.Threads = cores
	}

	// Setting up the concurrency to use.
	runtime.GOMAXPROCS(dumpOptions.Threads)

	// Creating the Task Manager.
	taskManager := tasks.NewTaskManager(
		&wgCreateChunks,
		&wgProcessChunks,
		cDataChunk,
		GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials),
		dumpOptions.Threads,
		dumpOptions.DestinationDir,
		dumpOptions.TablesWithoutPKOption)

	// Making the lists of tables. Either from a database or the tables paramenter.
	var tablesFromDatabases, tablesFromString, tablesToParse map[string]bool

	dbtm := GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)

	if len(flagDatabases) > 0 {
		tablesFromDatabases = sqlutils.TablesFromDatabase(flagDatabases, dbtm)
		log.Debugf("tablesFromDatabases: %v ", tablesFromDatabases)
	}

	if len(flagTables) > 0 {
		tablesFromString = sqlutils.TablesFromString(flagTables)
	}

	// Merging both lists (tables and databases)
	tablesToParse = tablesFromDatabases
	for table, _ := range tablesFromString {
		if _, ok := tablesToParse[table]; !ok {
			tablesToParse[table] = true
		}
	}

	// Adding the tasks to the task manager.
	// We create one task per table
	for table, _ := range tablesToParse {
		t := strings.Split(table, ".")
		taskManager.AddTask(tasks.NewTask(t[0], t[1], dumpOptions.ChunkSize, dbtm, &taskManager))
	}

	log.Debugf("Added %d connections to the taskManager", dumpOptions.Threads)

	// Creating the workers. One per thread from the parameters.
	for i := 0; i < dumpOptions.Threads; i++ {
		taskManager.AddWorkerDB(GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials))
	}
	// Creating the chunks from the tables.
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
