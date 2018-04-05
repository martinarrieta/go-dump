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
	"time"

	"github.com/martinarrieta/go-dump/go/utils"

	_ "github.com/go-sql-driver/mysql"
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

const AppVersion string = "0.01"

// GetMySQLConnection return the string to connect to the mysql server
func GetMySQLConnection(host *MySQLHost, credentials *MySQLCredentials) (*sql.DB, error) {
	var hoststring, userpass string
	userpass = fmt.Sprintf("%s:%s", credentials.User, credentials.Password)

	if len(host.SocketFile) > 0 {
		hoststring = fmt.Sprintf("unix(%s)", host.SocketFile)
	} else {
		hoststring = fmt.Sprintf("tcp(%s:%d)", host.HostName, host.Port)
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%s@%s/", userpass, hoststring))
	err = db.Ping()
	if err != nil {
		log.Fatal("Error")
	}

	return db, nil
}

type DumpOptions struct {
	MySQLHost             *MySQLHost
	MySQLCredentials      *MySQLCredentials
	Threads               int
	ChunkSize             int64
	OutputChunkSize       int64
	ChannelBufferSize     int
	LockTables            bool
	Debug                 bool
	TablesWithoutUKOption string
	DestinationDir        string
	GetMasterStatus       bool
	SkipUseDatabase       bool
}

func GetDumpOptions() *DumpOptions {
	return &DumpOptions{
		MySQLHost:        new(MySQLHost),
		MySQLCredentials: new(MySQLCredentials),
	}
}

func main() {
	startExecution := time.Now()

	var flagTables, flagDatabases string
	var flagHelp, flagVersion, flagDryRun, flagExecute, flagAllDatabases, flagAddDropTable bool
	dumpOptions := GetDumpOptions()

	flag.StringVar(&flagTables, "tables", "",
		"List of comma separated tables to dump.\n"+
			"Each table should have the database name included, "+
			"for example \"mydb.mytable,mydb2.mytable2\"")
	flag.StringVar(&flagDatabases, "databases", "",
		"List of comma separated databases to dump")
	flag.BoolVar(&flagAllDatabases, "all-databases", false, "Dump all the databases.")
	flag.StringVar(&dumpOptions.MySQLHost.HostName, "host",
		"localhost", "MySQL hostname")
	flag.StringVar(&dumpOptions.MySQLHost.SocketFile, "socket", "",
		"MySQL socket file")
	flag.IntVar(&dumpOptions.MySQLHost.Port, "port", 3306, "MySQL port number")
	flag.StringVar(&dumpOptions.MySQLCredentials.User, "mysql-user", "",
		"MySQL user name")
	flag.StringVar(&dumpOptions.MySQLCredentials.Password, "password", "",
		"MySQL password")
	flag.IntVar(&dumpOptions.Threads, "threads", 1, "Number of threads to use")
	flag.Int64Var(&dumpOptions.ChunkSize, "chunk-size", 1000,
		"Chunk size to get the rows")
	flag.Int64Var(&dumpOptions.OutputChunkSize, "output-chunk-size", 0,
		"Chunk size to output the rows")
	flag.IntVar(&dumpOptions.ChannelBufferSize, "channel-buffer-size", 1000,
		"Task channel buffer size")
	flag.BoolVar(&dumpOptions.LockTables, "lock-tables", true,
		"Lock tables to get consistent backup")
	flag.StringVar(&dumpOptions.TablesWithoutUKOption, "tables-without-uniquekey", "error",
		"Action to have with tables without any primary or unique key.\n"+
			"Valid actions are: 'error', 'skip', 'single-chunk'.")
	flag.BoolVar(&dumpOptions.Debug, "debug", false, "Display debug information.")
	flag.StringVar(&dumpOptions.DestinationDir, "destination", "",
		"Directory to store the dumps")
	flag.BoolVar(&flagHelp, "help", false, "Display this message")
	flag.BoolVar(&flagVersion, "version", false, "Display version and exit")
	flag.BoolVar(&flagDryRun, "dry-run", false, "Just calculate the number of chaunks per table and display it.")
	flag.BoolVar(&flagExecute, "execute", false, "Execute the dump.")
	flag.BoolVar(&dumpOptions.SkipUseDatabase, "skip-use-database", false, "Skip USE \"database\" in the dump.")
	flag.BoolVar(&dumpOptions.GetMasterStatus, "master-data", true, "Get the master data.")
	flag.BoolVar(&flagAddDropTable, "add-drop-table", false, "Add drop table before create table.")

	flag.Parse()

	if flagHelp == true {
		flag.Usage()
		return
	}
	if flagVersion == true {
		fmt.Println("go-dump version:", AppVersion)
		return
	}
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

	switch dumpOptions.TablesWithoutUKOption {
	case "error", "single-chunk":
		log.Debugf("The method to use with the tables without primary or unique key is \"%s\".",
			dumpOptions.TablesWithoutUKOption)
	case "skip":
		log.Fatalf("The option for --tables-without-pk \"skip\" is not implemented yet.")
	default:
		log.Fatalf("Error: \"%s\" is not a valid option for --tables-without-pk.",
			dumpOptions.TablesWithoutUKOption)
		flag.Usage()
	}

	if dumpOptions.DestinationDir == "" {
		log.Fatal("--destination dir is required")
	}

	// Setting OutputChunkSize to the same value as ChunkSize
	// if the OutputChunkSize is 0
	if dumpOptions.OutputChunkSize == 0 {
		dumpOptions.OutputChunkSize = dumpOptions.ChunkSize
	}

	// Creating the buffer for the channel
	cDataChunk := make(chan utils.DataChunk, dumpOptions.ChannelBufferSize)

	cores := runtime.NumCPU()
	if dumpOptions.Threads > cores {
		log.Warningf("The number of cores available is %d and the number of threads "+
			"requested were %d.We will set the number of cores to %d.",
			cores, dumpOptions.Threads, cores)
		dumpOptions.Threads = cores
	}

	// Setting up the concurrency to use.
	runtime.GOMAXPROCS(dumpOptions.Threads)

	tmdb, err := GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)
	if err != nil {
		log.Critical("Error whith the database connection. %s", err.Error())
	}
	// Creating the Task Manager.
	taskManager := utils.NewTaskManager(
		&wgCreateChunks,
		&wgProcessChunks,
		cDataChunk,
		tmdb,
		dumpOptions.Threads,
		dumpOptions.DestinationDir,
		dumpOptions.TablesWithoutUKOption,
		dumpOptions.SkipUseDatabase,
		dumpOptions.GetMasterStatus)

	// Making the lists of tables. Either from a database or the tables paramenter.
	var tablesFromDatabases, tablesFromString, tablesToParse map[string]bool

	dbtm, err := GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)
	dbchunks, err := GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)

	if err != nil {
		log.Critical("Error whith the database connection. %s", err.Error())
	}
	log.Debug("Error TablesFromDatabase: ", err)

	if flagAllDatabases == true {
		tablesToParse = utils.TablesFromAllDatabases(dbchunks)
	} else {
		if len(flagDatabases) > 0 {
			tablesFromDatabases = utils.TablesFromDatabase(flagDatabases, dbchunks)
			log.Debugf("tablesFromDatabases: %v ", tablesFromDatabases)
		}

		if len(flagTables) > 0 {
			tablesFromString = utils.TablesFromString(flagTables)
		}

		// Merging both lists (tables and databases)
		if len(tablesFromDatabases) > 0 {
			tablesToParse = tablesFromDatabases
		}
		if len(tablesFromString) > 0 {
			if len(tablesToParse) > 0 {
				for table, _ := range tablesFromString {
					if _, ok := tablesToParse[table]; !ok {
						tablesToParse[table] = true
					}
				}
			} else {
				tablesToParse = tablesFromString
			}
		}
	}

	// Adding the utils to the task manager.
	// We create one task per table
	for table, _ := range tablesToParse {
		t := strings.Split(table, ".")
		task := utils.NewTask(
			t[0], t[1],
			dumpOptions.ChunkSize,
			dumpOptions.OutputChunkSize,
			dbtm, &taskManager)
		taskManager.AddTask(task)
		log.Debugf("Table: %+v", task.Table)
	}

	log.Debugf("Added %d connections to the taskManager", dumpOptions.Threads)

	// Creating the workers. One per thread from the parameters.
	for i := 0; i < dumpOptions.Threads; i++ {
		conn, err := GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)
		if err != nil {
			log.Critical("Error whith the database connection. %s", err.Error())
		}
		conn.Ping()
		taskManager.AddWorkerDB(conn)
	}

	// Creating the chunks from the tables.
	taskManager.CreateChunksWaitGroup.Add(1)

	go taskManager.CreateChunks(dbchunks)

	if flagDryRun == true && flagExecute == true {
		log.Fatalf("Flags --dry-run and --execute are mutually exclusive")

	}
	go taskManager.PrintStatus()

	if flagDryRun == true {
		go taskManager.CleanChunkChannel()
		taskManager.CreateChunksWaitGroup.Wait()
		close(taskManager.ChunksChannel)
		taskManager.DisplaySummary()
	}

	if flagExecute == true {

		taskManager.GetTransactions(dumpOptions.LockTables, flagAllDatabases)
		if err := os.MkdirAll(dumpOptions.DestinationDir, 0755); err != nil {
			log.Fatalf("Error creating directory: %s\n%s",
				dumpOptions.DestinationDir, err.Error())
		}

		taskManager.StartWorkers()
		log.Debugf("ProcessChunksWaitGroup, %+v", taskManager.ProcessChunksWaitGroup)
		taskManager.CreateChunksWaitGroup.Wait()
		close(taskManager.ChunksChannel)
		taskManager.ProcessChunksWaitGroup.Wait()
		taskManager.WriteTablesSQL(flagAddDropTable)
		log.Info("Waiting for the creation of all the chunks.")
	}

	executionTime := time.Since(startExecution)

	log.Infof("Execution time: %s  ", executionTime.String())

}
