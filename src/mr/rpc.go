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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	Task      *Task
	ReduceNum int
}

type DistributePhase int
type TaskType int
type TaskStatus int

const (
	// 任务类型
	MapTask    = 1
	ReduceTask = 2
	// 阶段
	MapPhase    = 1
	ReducePhase = 2
	// 任务状态
	Ready    = 1
	Running  = 2
	Finished = 3
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
