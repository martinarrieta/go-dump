package utils

import (
	"database/sql"
	"os"
	"sync"
	"testing"
)

func getMySQLHost() *MySQLHost {
	return &MySQLHost{
		HostName: "localhost",
		Port:     3306,
	}
}

func getMySQLCredentials() *MySQLCredentials {
	return &MySQLCredentials{
		User:     "testuser",
		Password: "simpletest",
	}
}

func getDumpOptions() *DumpOptions {

	return &DumpOptions{
		MySQLHost:             getMySQLHost(),
		MySQLCredentials:      getMySQLCredentials(),
		Threads:               1,
		ChunkSize:             1000,
		OutputChunkSize:       1000,
		ChannelBufferSize:     1000,
		LockTables:            true,
		TablesWithoutUKOption: "single-chunk",
		DestinationDir:        "/tmp/testbackup",
		AddDropTable:          true,
		GetMasterStatus:       true,
		GetSlaveStatus:        false,
		SkipUseDatabase:       false,
		Compress:              false,
		CompressLevel:         0,
		IsolationLevel:        sql.LevelRepeatableRead,
		Consistent:            true,
	}
}

var dumpOptions = getDumpOptions()

var tmdb, _ = GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)

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
	dumpOptions)

func TestCreateTaskManager(t *testing.T) {
	if _, err := os.Stat(taskManager.DestinationDir); os.IsNotExist(err) {
		os.MkdirAll(taskManager.DestinationDir, 0755)
	}
	taskManager.AddWorkersDB()
	taskManager.GetTransactions(true, false)
}

func TestLoadIniFile(t *testing.T) {
	testOptions := getDumpOptions()

	skipUser := map[string]bool{"mysql-user": true}

	ParseIniFile("../../test/test.ini", testOptions, skipUser)

	if testOptions.Threads != 3 {
		t.Errorf("Threads should be 3")
	}

	if testOptions.MySQLCredentials.User != dumpOptions.MySQLCredentials.User {
		t.Errorf("MySQL user shouldn't change.")
	}
	return
}
