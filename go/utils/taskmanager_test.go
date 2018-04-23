package utils

import (
	"database/sql"
	"sync"
	"testing"
)

var defaultMySQLHost = MySQLHost{
	HostName: "localhost",
	Port:     3306,
}

var defaultMySQLCredentials = MySQLCredentials{
	User:     "testuser",
	Password: "simpletest",
}

var dumpOptions = DumpOptions{
	MySQLHost:             &defaultMySQLHost,
	MySQLCredentials:      &defaultMySQLCredentials,
	Threads:               1,
	ChunkSize:             1000,
	OutputChunkSize:       1000,
	ChannelBufferSize:     1000,
	LockTables:            true,
	TablesWithoutUKOption: "single-chunk",
	DestinationDir:        "/tmp/baskup",
	AddDropTable:          true,
	GetMasterStatus:       true,
	SkipUseDatabase:       false,
	Compress:              false,
	CompressLevel:         0,
	VerboseLevel:          0,
	IsolationLevel:        sql.LevelRepeatableRead,
	Consistent:            true,
}

var tmdb, err = GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)

// Creating the buffer for the channel
var cDataChunk = make(chan DataChunk, dumpOptions.ChannelBufferSize)

// WaitGroup for the creation of the chunks
var wgCreateChunks sync.WaitGroup

// WaitGroup to process the chunks
var wgProcessChunks sync.WaitGroup

var taskManager = NewTaskManager(
	&wgCreateChunks,
	&wgProcessChunks,
	cDataChunk,
	tmdb,
	&dumpOptions)

func TestCreateTaskManager(t *testing.T) {
	taskManager.AddWorkersDB()
	taskManager.GetTransactions(true, false)
}
