package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/martinarrieta/go-dump/go/utils"

	_ "github.com/go-sql-driver/mysql"
	"github.com/outbrain/golib/log"
)

// WaitGroup for the creation of the chunks
var wgCreateChunks sync.WaitGroup

// WaitGroup to process the chunks
var wgProcessChunks sync.WaitGroup

const AppVersion string = "0.01"

func GetDumpOptions() *utils.DumpOptions {
	return &utils.DumpOptions{
		MySQLHost:        new(utils.MySQLHost),
		MySQLCredentials: new(utils.MySQLCredentials),
	}
}

func printOption(w io.Writer, f *flag.Flag) {
	fmt.Fprint(w, "   --", f.Name, "\t", f.Usage)

	if f.DefValue != "" {
		fmt.Fprint(w, " Default [", f.DefValue, "]")
	}

	fmt.Fprint(w, "\n")

}
func PrintUsage(flags map[string]*flag.Flag) {

	w := tabwriter.NewWriter(os.Stdout, 30, 0, 1, ' ', tabwriter.TabIndent)
	fmt.Fprintln(w, "Usage: go-dump  --destination path [--databases str] [--tables str] [--all-databases] [--dry-run | --execute ] [--help] [--debug] [--quiet] [--version] [--lock-tables] [--consistent] [--isolation-level str] [--channel-buffer-size num] [--chunk-size num] [--tables-without-uniquekey str] [--threads num] [--mysql-user str] [--mysql-password str] [--mysql-host str] [--mysql-port num] [--mysql-socket path] [--add-drop-table] [--get-master-status] [--get-slave-status] [--output-chunk-size num] [--skip-use-database] [--compress] [--compress-level] [--ini-files str]")

	fmt.Fprintln(w, "go-dump dumps a database or a table from a MySQL server and creates the SQL statements to recreate a table. This tool create one file per table per thread in the destination directory")
	fmt.Fprint(w, "Example: go-dump --destination /tmp/dbdump --databases mydb --mysql-user myuser --mysql-password password\n\n")
	fmt.Fprint(w, "Options description\n\n")

	fmt.Fprintln(w, "# General:")
	for _, opt := range []string{"help", "dry-run", "execute", "debug", "quiet", "version",
		"lock-tables", "channel-buffer-size", "chunk-size", "tables-without-uniquekey",
		"threads", "compress", "compress-level", "consistent", "isolation-level", "ini-file"} {
		printOption(w, flags[opt])
	}

	fmt.Fprintln(w, "\n# MySQL options:")
	for _, opt := range []string{"mysql-user", "mysql-password", "mysql-host", "mysql-port", "mysql-socket"} {
		printOption(w, flags[opt])
	}

	fmt.Fprintln(w, "\n# Databases or tables to dump:")
	for _, opt := range []string{"all-databases", "databases", "tables"} {
		printOption(w, flags[opt])
	}
	fmt.Fprintln(w, "\n# Output options:")
	for _, opt := range []string{"destination", "add-drop-table", "get-master-status", "get-slave-status", "output-chunk-size", "skip-use-database"} {
		printOption(w, flags[opt])
	}
	w.Flush()
}

var dumpOptions = GetDumpOptions()
var flagSet = make(map[string]bool)

func main() {
	startExecution := time.Now()

	var (
		flagHelp, flagVersion bool
		flagIniFile           string
	)

	var consitent = true
	flag.StringVar(&dumpOptions.TemporalOptions.Tables, "tables", "", "List of comma separated tables to dump. Each table should have the database name included, for example \"mydb.mytable,mydb2.mytable2\".")
	flag.StringVar(&dumpOptions.TemporalOptions.Databases, "databases", "", "List of comma separated databases to dump.")
	flag.BoolVar(&dumpOptions.TemporalOptions.AllDatabases, "all-databases", false, "Dump all the databases.")
	flag.StringVar(&dumpOptions.MySQLHost.HostName, "mysql-host", "localhost", "MySQL hostname.")
	flag.StringVar(&dumpOptions.MySQLHost.SocketFile, "mysql-socket", "", "MySQL socket file.")
	flag.IntVar(&dumpOptions.MySQLHost.Port, "mysql-port", 3306, "MySQL port number")
	flag.StringVar(&dumpOptions.MySQLCredentials.User, "mysql-user", "root", "MySQL user name.")
	flag.StringVar(&dumpOptions.MySQLCredentials.Password, "mysql-password", "", "MySQL password.")
	flag.IntVar(&dumpOptions.Threads, "threads", 1, "Number of threads to use.")
	flag.Uint64Var(&dumpOptions.ChunkSize, "chunk-size", 1000, "Chunk size to get the rows.")
	flag.Uint64Var(&dumpOptions.OutputChunkSize, "output-chunk-size", 0, "Chunk size to output the rows.")
	flag.IntVar(&dumpOptions.ChannelBufferSize, "channel-buffer-size", 1000, "Task channel buffer size.")
	flag.BoolVar(&dumpOptions.LockTables, "lock-tables", true, "Lock tables to get consistent backup.")
	flag.StringVar(&dumpOptions.TablesWithoutUKOption, "tables-without-uniquekey", "error", "Action to have with tables without any primary or unique key. Valid actions are: 'error', 'single-chunk'.")
	flag.BoolVar(&dumpOptions.TemporalOptions.Debug, "debug", false, "Display debug information.")
	flag.StringVar(&dumpOptions.DestinationDir, "destination", "", "Directory to store the dumps.")
	flag.BoolVar(&flagHelp, "help", false, "Display this message.")
	flag.BoolVar(&flagVersion, "version", false, "Display version and exit.")
	flag.BoolVar(&dumpOptions.TemporalOptions.DryRun, "dry-run", false, "Just calculate the number of chaunks per table and display it.")
	flag.BoolVar(&dumpOptions.TemporalOptions.Execute, "execute", false, "Execute the dump.")
	flag.BoolVar(&dumpOptions.SkipUseDatabase, "skip-use-database", false, "Skip USE \"database\" in the dump.")
	flag.BoolVar(&dumpOptions.GetMasterStatus, "get-master-status", false, "Get the master data.")
	flag.BoolVar(&dumpOptions.GetSlaveStatus, "get-slave-status", false, "Get the slave data.")
	flag.BoolVar(&dumpOptions.AddDropTable, "add-drop-table", false, "Add drop table before create table.")
	flag.BoolVar(&dumpOptions.Compress, "compress", false, "Enable compression to the output files.")
	flag.IntVar(&dumpOptions.CompressLevel, "compress-level", 1, "Compression level from 1 (best speed) to 9 (best compression).")
	flag.BoolVar(&dumpOptions.TemporalOptions.Quiet, "quiet", false, "Do not display INFO messages during the process.")
	flag.StringVar(&dumpOptions.TemporalOptions.IsolationLevel, "isolation-level", "REPEATABLE READ", "Isolation level to use. If you need a consitent backup, leave the default 'REPEATABLE READ', other options READ COMMITTED, READ UNCOMMITTED and SERIALIZABLE.")
	flag.BoolVar(&dumpOptions.Consistent, "consistent", true, "Get a consistent backup.")
	flag.StringVar(&flagIniFile, "ini-file", "", "INI file to read the configuration options.")

	flag.Parse()

	// Collect the flags that were assigned from the command line.
	flag.Visit(func(f *flag.Flag) { flagSet[f.Name] = true })

	// Parse the ini file.
	if flagIniFile != "" {
		utils.ParseIniFile(flagIniFile, dumpOptions, flagSet)
	}

	flags := make(map[string]*flag.Flag)

	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		flags[f.Name] = f
	})

	// Print the help message and exit.
	if flagHelp {
		PrintUsage(flags)
		return
	}

	// Print the version and exit.
	if flagVersion {
		fmt.Println("go-dump version:", AppVersion)
		return
	}

	//Setting debug level
	if dumpOptions.TemporalOptions.Debug {
		log.SetLevel(log.DEBUG)
	} else if dumpOptions.TemporalOptions.Quiet {
		log.SetLevel(log.WARNING)
	} else {
		log.SetLevel(log.INFO)
	}

	// Added the signal for the `Ctl c` command.
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Fatalf("Killing the dumper.")
	}()

	// Parsed TablesWithoutUKOption options
	switch dumpOptions.TablesWithoutUKOption {
	case "error", "single-chunk":
		log.Debugf("The method to use with the tables without primary or unique key is \"%s\".",
			dumpOptions.TablesWithoutUKOption)
	case "skip":
		log.Fatalf("The option for --tables-without-pk \"skip\" is not implemented yet.")
	default:
		log.Fatalf("Error: \"%s\" is not a valid option for --tables-without-pk.",
			dumpOptions.TablesWithoutUKOption)
		PrintUsage(flags)
	}

	// Making sure that if LockTables is false, consistent must be false as well.
	if !dumpOptions.LockTables && dumpOptions.Consistent {
		log.Fatalf("Lock tables is required to get a consitent backup. Use --help for more information.")
	}

	if dumpOptions.DestinationDir == "" {
		log.Fatal("--destination dir is required, use --help for more information.")
	}

	// Parsed isolation level.
	switch strings.ToUpper(dumpOptions.TemporalOptions.IsolationLevel) {
	case "SERIALIZABLE":
		dumpOptions.IsolationLevel = sql.LevelSerializable
	case "REPEATABLE READ":
		dumpOptions.IsolationLevel = sql.LevelRepeatableRead
	case "READ COMMITTED":
		dumpOptions.IsolationLevel = sql.LevelReadCommitted
		consitent = false
	case "READ UNCOMMITTED":
		dumpOptions.IsolationLevel = sql.LevelReadUncommitted
		consitent = false
	default:
		log.Fatalf("Unknown issolation level %s. Use --help for more information.", dumpOptions.TemporalOptions.IsolationLevel)
	}

	// Parsed consistent and making sure taht the isolation level is correct.
	if !consitent && dumpOptions.Consistent {
		log.Fatalf("Isolation level \"%s\" is not compatible with the --consitent option. Use --help for more information about both options.")
	}
	// Setting OutputChunkSize to the same value as ChunkSize
	// if the OutputChunkSize is 0
	if dumpOptions.OutputChunkSize == 0 {
		dumpOptions.OutputChunkSize = dumpOptions.ChunkSize
	}

	if dumpOptions.CompressLevel < 1 || dumpOptions.CompressLevel > 9 {
		log.Fatal("The option --compress-level must be a number between 1 and 9")
	}

	// Creating the buffer for the channel
	cDataChunk := make(chan utils.DataChunk, dumpOptions.ChannelBufferSize)

	// Checking the number of cores and comparing with the threads option.
	cores := runtime.NumCPU()
	if dumpOptions.Threads > cores {
		log.Warningf("The number of cores available is %d and the number of threads requested were %d.",
			cores, dumpOptions.Threads)
	}

	// Setting up the concurrency to use.
	runtime.GOMAXPROCS(dumpOptions.Threads)

	tmdb, err := utils.GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)
	if err != nil {
		log.Critical("Error whith the database connection. %s", err.Error())
	}
	// Creating the Task Manager.
	taskManager := utils.NewTaskManager(
		&wgCreateChunks,
		&wgProcessChunks,
		cDataChunk,
		tmdb,
		dumpOptions)

	// Making the lists of tables. Either from a database or the tables paramenter.
	var tablesFromDatabases, tablesFromString, tablesToParse map[string]bool

	dbchunks, err := utils.GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)

	if err != nil {
		log.Critical("Error whith the database connection. %s", err.Error())
	}
	log.Debug("Error TablesFromDatabase: ", err)

	if dumpOptions.TemporalOptions.AllDatabases {
		tablesToParse = utils.TablesFromAllDatabases(dbchunks)
	} else {
		if len(dumpOptions.TemporalOptions.Databases) > 0 {
			tablesFromDatabases = utils.TablesFromDatabase(dumpOptions.TemporalOptions.Databases, dbchunks)
			log.Debugf("tablesFromDatabases: %v ", tablesFromDatabases)
		}

		if len(dumpOptions.TemporalOptions.Tables) > 0 {
			tablesFromString = utils.TablesFromString(dumpOptions.TemporalOptions.Tables)
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
			&taskManager)
		task.PrintInfo()
		taskManager.AddTask(&task)
		log.Debugf("Table: %+v", task.Table)
	}

	log.Debugf("Added %d connections to the taskManager", dumpOptions.Threads)

	taskManager.AddWorkersDB()

	// Creating the chunks from the tables.
	taskManager.CreateChunksWaitGroup.Add(1)

	go taskManager.CreateChunks(dbchunks)

	if dumpOptions.TemporalOptions.DryRun && dumpOptions.TemporalOptions.Execute {
		log.Fatalf("Flags --dry-run and --execute are mutually exclusive")

	}
	go taskManager.PrintStatus()

	if dumpOptions.TemporalOptions.DryRun {
		go taskManager.CleanChunkChannel()
		taskManager.CreateChunksWaitGroup.Wait()
		close(taskManager.ChunksChannel)
		taskManager.DisplaySummary()
	}

	if dumpOptions.TemporalOptions.Execute {
		if err := os.MkdirAll(dumpOptions.DestinationDir, 0755); err != nil {
			log.Fatalf("Error creating directory: %s\n%s",
				dumpOptions.DestinationDir, err.Error())
		}
		taskManager.GetTransactions(dumpOptions.LockTables, dumpOptions.TemporalOptions.AllDatabases)

		taskManager.StartWorkers()
		log.Debugf("ProcessChunksWaitGroup, %+v", taskManager.ProcessChunksWaitGroup)
		taskManager.CreateChunksWaitGroup.Wait()
		close(taskManager.ChunksChannel)
		taskManager.ProcessChunksWaitGroup.Wait()
		taskManager.WriteTablesSQL(dumpOptions.AddDropTable)
		log.Info("Waiting for the creation of all the chunks.")
	}

	executionTime := time.Since(startExecution)

	log.Infof("Execution time: %s  ", executionTime.String())

}
