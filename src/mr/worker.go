package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	reply := &RequestReply{}
	dummy := &InformFinish{}
	request := &RequestTask{}
	requestCount := 0
	for {
		request.RequestCount = requestCount
		if !call("Master.Assign", request, reply) {
			log.Printf("Can't hear from the master")
			continue
		}
		num, _ := strconv.Atoi(reply.Num)
		if reply.Task == Wait {
			if debug {
				log.Printf("worker waiting")
			}
			time.Sleep(time.Second)
		} else if reply.Task == Exit {
			if context {
				log.Printf("worker exits")
			}
			break
		} else if reply.Task == MapTask {
			if debug {
				log.Printf("start for map %d...\n", num)
			}
			intermediate := mapf(reply.FileName, fileContent(reply.FileName))
			completeMap(intermediate, int(reply.NReduce), num)
			call("Master.FinishMap", &InformFinish{num}, dummy)
		} else if reply.Task == RedTask {
			if debug {
				log.Printf("[request count: %d] start for reduce %d...\n", requestCount, num)
			}
			completeRed(num, reducef)
			call("Master.FinishRed", &InformFinish{num}, dummy)
		} else {
			panic("unexpected task!")
		}
		requestCount++
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func fileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file) // content is a []byte
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	return string(content)
}

func completeMap(intermediate []KeyValue, nReduce int, num int) {
	sort.Sort(ByKey(intermediate))
	tfile := make([]*os.File, nReduce)
	for i := 0; i < len(intermediate); {
		j := i + 1
		key := intermediate[i].Key
		for j < len(intermediate) && intermediate[j].Key == key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		redNum := ihash(key) % nReduce

		if tfile[redNum] == nil {
			tempName := fmt.Sprintf("temp-mr-*")
			tfile[redNum], _ = ioutil.TempFile("./", tempName)
		}
		// the design of reduce under mrapps/ are not suitable for combining
		fmt.Fprintf(tfile[redNum], "%v\t%s\n", key, strings.Join(values, " "))
		i = j
	}

	for i, tf := range tfile {
		if tf != nil {
			tf.Close()
			os.Rename(tf.Name(), fmt.Sprintf("mr-%d-%d", num, i))
		}
	}
}

func completeRed(redNum int, reducef func(string, []string) string) {
	// suppose all the intermediate files have name mr-X-Y
	pattern := fmt.Sprintf("mr-*-%d", redNum)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("can't find intermediate files for reducer %d", redNum)
	}
	kv := map[string][]string{}
	ofile, _ := ioutil.TempFile("./", "temp-mr-out-*")
	for _, ifile := range matches {
		// if debug {
		// 	log.Printf("worker processing %s", ifile)
		// }
		// read line by line
		file, err := os.Open(ifile)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			// one key per line
			var line = scanner.Text()
			keyvalues := strings.Split(line, "\t")
			if len(keyvalues) < 2 {
				log.Fatalf("filename: %s\tUnexpected line: %s\n", ifile, line)
			}
			key := keyvalues[0]
			_, ok := kv[key]
			if !ok {
				kv[key] = []string{}
			}
			values := strings.Split(keyvalues[1], " ")
			kv[key] = append(kv[key], values...)
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}
	if detail {
		log.Printf("worker collected values for %d", redNum)
	}
	for k, v := range kv {
		if detail {
			log.Printf("worker waiting for reducef %s", k)
		}
		output := reducef(k, v)
		_, oerr := fmt.Fprintf(ofile, "%v %v\n", k, output)
		if oerr != nil {
			log.Fatalf("Fail to write red output %s", ofile.Name())
		}
	}
	oerr := ofile.Close()
	if oerr != nil {
		log.Fatalf("Fail to close red output %s", ofile.Name())
	}
	if detail {
		log.Printf("worker almost finish %d", redNum)
	}
	oname := fmt.Sprintf("mr-out-%d", redNum)
	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		log.Fatalf("rename of %s to %s fails!", ofile.Name(), oname)
	}
	if context {
		log.Printf("%s has been created\n", oname)
	}
}
