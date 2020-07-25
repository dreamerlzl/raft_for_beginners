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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	MapTask TaskType = iota + 1
	RedTask
	Exit
	Wait
)

type RequestTask struct {
	RequestCount int
}

type RequestReply struct {
	Task     TaskType
	FileName string // one split per map task,
	// since the master doesn't know how many workers there are
	Num     string // a detail to hide, for specifying the map task num/reduce input
	NReduce int    // to tell workers how many reducers there are
}

type InformFinish struct {
	Num int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
