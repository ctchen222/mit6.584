package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	JobFinished
)

func (t TaskType) String() string {
	switch t {
	case MapTask:
		return "MapTask"
	case ReduceTask:
		return "ReduceTask"
	case WaitingTask:
		return "WaitingTask"
	case JobFinished:
		return "JobFinished"
	default:
		return "Unknown"
	}
}

// Add your RPC definitions here.
type WorkerArgs struct {
	MapTaskNumber    int
	ReduceTaskNumber int
}

type WorkerReply struct {
	Tasktype int // 0: map task, 1: reduce task, 2: waiting, 3: job finished
	NMap     int // number of map task
	NReduce  int // number of reduce task

	MapTaskNumber int    // map task only
	Filename      string // maptask only

	ReduceTaskNumber int // reducetask only
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
