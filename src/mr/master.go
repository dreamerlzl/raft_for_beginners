package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var debug = false
var context = true
var lock = false
var detail = false

type Master struct {
	// Your definitions here.
	muMap       sync.Mutex // mutex for map task
	muMapCount  sync.Mutex
	splits      []string   // all the map task input splits
	mapChan     chan int   // a queue for map tasks
	finishedMap int        // the number of finished map tasks
	mapFinished []bool     // to see whether need to reissue
	muRed       sync.Mutex // mutex for red task
	muRedCount  sync.Mutex
	nReduce     int      // number of reducers
	redChan     chan int // a queue for red tasks
	finishedRed int      // the number of finished red tasks
	redFinished []bool   // to see whether need to reissue the red task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	// if lock {
	// 	log.Printf("Done waits for muRedCount")
	// 	defer log.Printf("Done releases muRedCount")
	// }
	m.muRedCount.Lock()
	defer m.muRedCount.Unlock()
	ret = (m.finishedRed == m.nReduce)
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	m.splits = files
	m.mapFinished = make([]bool, len(files))
	m.redFinished = make([]bool, nReduce)
	m.mapChan = make(chan int, len(files))
	m.redChan = make(chan int, nReduce)
	for i := 0; i < len(files); i++ {
		m.mapChan <- i
	}
	for i := 0; i < nReduce; i++ {
		m.redChan <- i
	}
	// delete all intermediate output generated last time
	files, err := filepath.Glob("./mr-*-*")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		match, _ := regexp.MatchString("mr-\\d+-\\d+", f)
		if !match {
			continue
		}
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}
	m.server()
	return &m
}

func (m *Master) Assign(args *RequestTask, reply *RequestReply) error {
	// first check whether there are map tasks left
	if lock {
		log.Printf("%d waits for muMap\n", args.RequestCount)
	}
	m.muMap.Lock()
	select {
	case num := <-m.mapChan:
		// reply.Num = strconv.Itoa(num)
		reply.Num = strconv.Itoa(num)
		if debug {
			log.Printf("issue map task: %v", reply.Num)
		}
		reply.NReduce = m.nReduce
		reply.Task = MapTask
		reply.FileName = m.splits[num] // one split per map task
		m.muMap.Unlock()
		if lock {
			log.Printf("%d releases muMap\n", args.RequestCount)
		}
		go m.waitMap(num)
		return nil
	default:
		if m.finishedMap < len(m.splits) {
			reply.Task = Wait
			m.muMap.Unlock()
			if lock {
				log.Printf("%d releases muMap\n", args.RequestCount)
			}
			return nil
		}
	}
	if lock {
		log.Printf("%d releases muMap\n", args.RequestCount)
	}
	m.muMap.Unlock()
	// // wait until the map phase is done
	// m.mapCond.L.Lock()
	// for m.finishedMap < len(m.splits) {
	// 	m.mapCond.Wait()
	// }
	// m.mapCond.Broadcast()
	// m.mapCond.L.Unlock()

	// then check whether there are red tasks left
	if lock {
		log.Printf("%d waits for muRed\n", args.RequestCount)
	}
	m.muRed.Lock()
	if lock {
		defer log.Printf("%d releases muRed\n", args.RequestCount)
	}
	defer m.muRed.Unlock()
	select {
	case num := <-m.redChan:
		// reply.Num = strconv.Itoa(num)
		reply.Num = strconv.Itoa(num)
		if debug {
			log.Printf("issue red task %v for request count: %d\n", reply.Num, args.RequestCount)
		}
		reply.Task = RedTask
		go m.waitRed(num)
	default:
		if m.finishedRed < m.nReduce {
			reply.Task = Wait
		} else {
			reply.Task = Exit
		}
	}
	return nil
}

func (m *Master) waitMap(num int) {
	time.Sleep(10 * time.Second)
	if !m.mapFinished[num] {
		m.muMap.Lock()
		m.mapChan <- num
		if debug {
			log.Printf("reissue map task %d\n", num)
		}
		m.muMap.Unlock()
	}
}

func (m *Master) waitRed(num int) {
	time.Sleep(10 * time.Second)
	if !m.redFinished[num] {
		m.muRed.Lock()
		m.redChan <- num
		if debug {
			log.Printf("reissue reduce task %d\n", num)
		}
		m.muRed.Unlock()
	}
}

func (m *Master) FinishMap(args *InformFinish, reply *RequestReply) error {
	if lock {
		log.Printf("FinishMap %d waits for muMapCount", args.Num)
		defer log.Printf("FinishMap %d releases muMapCount", args.Num)
	}
	m.muMapCount.Lock()
	defer m.muMapCount.Unlock()
	m.mapFinished[args.Num] = true
	m.finishedMap++
	if context {
		log.Printf("map task %d is finished\n", args.Num)
	}
	return nil
}

func (m *Master) FinishRed(args *InformFinish, reply *RequestReply) error {
	if lock {
		log.Printf("FinishRed %d waits for muRedCount", args.Num)
		defer log.Printf("FinishRed %d releases muRedCount", args.Num)
	}
	m.muRedCount.Lock()
	defer m.muRedCount.Unlock()
	m.redFinished[args.Num] = true
	m.finishedRed++
	if context {
		log.Printf("red task %d is finished\n", args.Num)
	}
	return nil
}
