package utils

import "testing"

var taskManager = TaskManager{
	ThreadsCount:          4,
	DestinationDir:        "/tmp/out",
	TablesWithoutPKOption: "single-chunk",
	SkipUseDatabase:       true,
	GetMasterStatus:       true,
	databaseEngines:       make(map[string]*Table),
}

func TestTaskmanager(t *testing.T) {

	taskManager.AddTask(&task1)
	taskManager.AddTask(&task2)
	if len(taskManager.GetTasksPool()) != 2 {
		t.Fatal("TaskPook is not 1")
	}
}
