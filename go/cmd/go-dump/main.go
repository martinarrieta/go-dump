package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"gopkg.in/ini.v1"

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
	fmt.Fprintln(w, "Usage: go-dump  --destination path [--databases str] [--tables str] [--all-databases] [--dry-run | --execute ] [--help] [--debug] [--quiet] [--version] [--lock-tables] [--consistent] [--isolation-level str] [--channel-buffer-size num] [--chunk-size num] [--tables-without-uniquekey str] [--threads num] [--mysql-user str] [--mysql-password str] [--mysql-host str] [--mysql-port num] [--mysql-socket path] [--add-drop-table] [--get-master-status] [--get-slave-status] [--output-chunk-size num] [--skip-use-database] [--compress] [--compress-level]")

	fmt.Fprintln(w, "go-dump dumps a database or a table from a MySQL server and creates the SQL statements to recreate a table. This tool create one file per table per thread in the destination directory")
	fmt.Fprint(w, "Example: go-dump --destination /tmp/dbdump --databases mydb --mysql-user myuser --mysql-password password\n\n")
	fmt.Fprint(w, "Options description\n\n")

	fmt.Fprintln(w, "# General:")
	for _, opt := range []string{"help", "dry-run", "execute", "debug", "quiet", "version",
		"lock-tables", "channel-buffer-size", "chunk-size", "tables-without-uniquekey",
		"threads", "compress", "compress-level"} {
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

func parseMySQLIniOptions(section *ini.Section) {
	var err error
	for key := range section.Keys() {
		if flagSet["mysql-"+section.Keys()[key].Name()] {
			log.Debugf("Skip key %s", section.Keys()[key].Name())
			continue
		}
		log.Debugf("Continue key %s", section.Keys()[key].Name())

		switch section.Keys()[key].Name() {
		case "user":
			dumpOptions.MySQLCredentials.User = section.Keys()[key].Value()
		case "password":
			dumpOptions.MySQLCredentials.Password = section.Keys()[key].Value()
		case "host":
			dumpOptions.MySQLHost.HostName = section.Keys()[key].Value()
		case "port":
			dumpOptions.MySQLHost.Port, err = strconv.Atoi(section.Keys()[key].Value())
			if err != nil {
				log.Fatalf("Port number %s can not be converted to integer. Error: %s", section.Keys()[key].Value(), err.Error())
			}
		case "socket":
			dumpOptions.MySQLHost.SocketFile = section.Keys()[key].Value()
		}
	}
}

func parseIniOptions(section *ini.Section) {
	var errInt, errBool error
	for key := range section.Keys() {
		if flagSet[section.Keys()[key].Name()] {
			log.Debugf("Skip key %s", section.Keys()[key].Name())
			continue
		}
		log.Debugf("Continue key %s", section.Keys()[key].Name())

		switch section.Keys()[key].Name() {
		case "mysql-user":
			dumpOptions.MySQLCredentials.User = section.Keys()[key].Value()
		case "mysql-password":
			dumpOptions.MySQLCredentials.Password = section.Keys()[key].Value()
		case "mysql-host":
			dumpOptions.MySQLHost.HostName = section.Keys()[key].Value()
		case "mysql-port":
			dumpOptions.MySQLHost.Port, errInt = strconv.Atoi(section.Keys()[key].Value())
		case "mysql-socket":
			dumpOptions.MySQLHost.SocketFile = section.Keys()[key].Value()
		case "threads":
			dumpOptions.Threads, errInt = strconv.Atoi(section.Keys()[key].Value())
		case "chunk-size":
			dumpOptions.ChunkSize, errInt = strconv.ParseUint(section.Keys()[key].Value(), 10, 64)
		case "output-chunk-size":
			dumpOptions.OutputChunkSize, errInt = strconv.ParseUint(section.Keys()[key].Value(), 10, 64)
		case "lock-tables":
			dumpOptions.LockTables, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "tables-without-uniquekey":
			dumpOptions.TablesWithoutUKOption = section.Keys()[key].Value()
		case "debug":
			dumpOptions.LockTables, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "destination":
			dumpOptions.DestinationDir = section.Keys()[key].Value()
		case "skip-use-database":
			dumpOptions.SkipUseDatabase, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "get-master-status":
			dumpOptions.GetMasterStatus, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "get-slave-status":
			dumpOptions.LockTables, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "add-drop-table":
			dumpOptions.AddDropTable, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "compress":
			dumpOptions.Compress, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "compress-level":
			dumpOptions.CompressLevel, errInt = strconv.Atoi(section.Keys()[key].Value())
		case "isolation-level":
			//dumpOptions.IsolationLevel, errInt = strconv.Atoi(section.Keys()[key].Value())
		case "consistent":
			dumpOptions.Consistent, errBool = strconv.ParseBool(section.Keys()[key].Value())
		default:
			log.Warningf("Unknown option %s", section.Keys()[key].Name())
		}

		if errInt != nil {
			log.Fatalf("Variable %s with the value %s can not be converted to integer. Error: %s",
				section.Keys()[key].Name(), section.Keys()[key].Value(), errInt.Error())
		}
		if errBool != nil {
			log.Fatalf("Variable %s with the value %s can not be converted to boolean. Error: %s",
				section.Keys()[key].Name(), section.Keys()[key].Value(), errBool.Error())
		}
	}
}

var dumpOptions = GetDumpOptions()
var flagSet = make(map[string]bool)

func main() {
	startExecution := time.Now()

	var (
		flagTables, flagDatabases, flagIsolationLevel, flagIniFile string
		flagHelp, flagVersion, flagDryRun                          bool
		flagExecute, flagAllDatabases                              bool
		flagAddDropTable, flagQuiet, flagDebug                     bool
	)

	var consitent = true
	flag.StringVar(&flagTables, "tables", "", "List of comma separated tables to dump. Each table should have the database name included, for example \"mydb.mytable,mydb2.mytable2\".")
	flag.StringVar(&flagDatabases, "databases", "", "List of comma separated databases to dump.")
	flag.BoolVar(&flagAllDatabases, "all-databases", false, "Dump all the databases.")
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
	flag.BoolVar(&flagDebug, "debug", false, "Display debug information.")
	flag.StringVar(&dumpOptions.DestinationDir, "destination", "", "Directory to store the dumps.")
	flag.BoolVar(&flagHelp, "help", false, "Display this message.")
	flag.BoolVar(&flagVersion, "version", false, "Display version and exit.")
	flag.BoolVar(&flagDryRun, "dry-run", false, "Just calculate the number of chaunks per table and display it.")
	flag.BoolVar(&flagExecute, "execute", false, "Execute the dump.")
	flag.BoolVar(&dumpOptions.SkipUseDatabase, "skip-use-database", false, "Skip USE \"database\" in the dump.")
	flag.BoolVar(&dumpOptions.GetMasterStatus, "get-master-status", true, "Get the master data.")
	flag.BoolVar(&dumpOptions.GetSlaveStatus, "get-slave-status", false, "Get the slave data.")
	flag.BoolVar(&dumpOptions.AddDropTable, "add-drop-table", false, "Add drop table before create table.")
	flag.BoolVar(&dumpOptions.Compress, "compress", false, "Enable compression to the output files.")
	flag.IntVar(&dumpOptions.CompressLevel, "compress-level", 1, "Compression level from 1 (best speed) to 9 (best compression).")
	flag.IntVar(&dumpOptions.VerboseLevel, "verbose-level", 1, "Compression level from 1 (best speed) to 9 (best compression).")
	flag.BoolVar(&flagQuiet, "quiet", false, "Do not display INFO messages during the process.")
	flag.StringVar(&flagIsolationLevel, "isolation-level", "REPEATABLE READ", "Isolation level to use. If you need a consitent backup, leave the default 'REPEATABLE READ', other options READ COMMITTED, READ UNCOMMITTED and SERIALIZABLE.")
	flag.BoolVar(&dumpOptions.Consistent, "consistent", true, "Get a consistent backup.")
	flag.StringVar(&flagIniFile, "ini-file", "", "INI file to read the configuration options.")

	flag.Parse()

	// Collect the flags that were assigned from the command line.
	flag.Visit(func(f *flag.Flag) { flagSet[f.Name] = true })

	// Parse the ini file.
	if flagIniFile != "" {
		cfg, err := ini.Load(flagIniFile)
		if err != nil {
			os.Exit(1)
		}

		// Check the different sections in the ini file
		for section := range cfg.Sections() {
			cfg.Sections()[section].Name()
			switch cfg.Sections()[section].Name() {
			case "client", "mysqldump":
				parseMySQLIniOptions(cfg.Sections()[section])
			case "go-dump":
				parseIniOptions(cfg.Sections()[section])
			}
		}
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
	if flagDebug {
		log.SetLevel(log.DEBUG)
	} else if flagQuiet {
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
	switch strings.ToUpper(flagIsolationLevel) {
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
		log.Fatalf("Unknown issolation level %s. Use --help for more information.", flagIsolationLevel)
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

	if flagAllDatabases {
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

	if flagDryRun && flagExecute {
		log.Fatalf("Flags --dry-run and --execute are mutually exclusive")

	}
	go taskManager.PrintStatus()

	if flagDryRun {
		go taskManager.CleanChunkChannel()
		taskManager.CreateChunksWaitGroup.Wait()
		close(taskManager.ChunksChannel)
		taskManager.DisplaySummary()
	}

	if flagExecute {
		if err := os.MkdirAll(dumpOptions.DestinationDir, 0755); err != nil {
			log.Fatalf("Error creating directory: %s\n%s",
				dumpOptions.DestinationDir, err.Error())
		}
		taskManager.GetTransactions(dumpOptions.LockTables, flagAllDatabases)

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
