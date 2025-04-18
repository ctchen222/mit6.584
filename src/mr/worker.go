package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		args := WorkerArgs{}
		reply := WorkerReply{}
		ok := call("Coordinator.AllocateTask", &args, &reply)
		if !ok || reply.Tasktype == 3 {
			break
		}
		if reply.Tasktype == 0 {
			intermediate := []KeyValue{}

			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()

			kva := mapf(reply.Filename, string(content))
			// fmt.Printf("kva %v\n", kva)
			intermediate = append(intermediate, kva...)
			// fmt.Printf("intermediate %v\n", intermediate)

			buckets := make([][]KeyValue, reply.NReduce)
			for i := range buckets {
				buckets[i] = []KeyValue{}
			}
			for _, kva := range intermediate {
				buckets[ihash(kva.Key)%reply.NReduce] = append(buckets[ihash(kva.Key)%reply.NReduce], kva)
			}

			// write into intermediate files
			for i := range buckets {
				oname := "mr-" + strconv.Itoa(reply.MapTaskNumber) + "-" + strconv.Itoa(i)
				ofile, _ := ioutil.TempFile("", oname+"*")
				enc := json.NewEncoder(ofile)
				for _, kva := range buckets[i] {
					err := enc.Encode(&kva)
					if err != nil {
						log.Fatalf("cannot write into %v", oname)
					}
				}
				os.Rename(ofile.Name(), oname)
				ofile.Close()
			}
			// call master to send the finish message
			finishedArgs := WorkerArgs{reply.MapTaskNumber, -1}
			finishedReply := WorkerReply{}
			call("Coordinator.ReceiveFinishedMap", &finishedArgs, &finishedReply)
		} else if reply.Tasktype == 1 {
			// reduce task
			// collect key-value from mr-X-Y
			intermediate := []KeyValue{}
			for i := 0; i < reply.NMap; i++ {
				iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNumber)
				// open && read the file
				file, err := os.Open(iname)
				if err != nil {
					log.Fatalf("cannot open %v", file)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			// sort by key
			sort.Sort(ByKey(intermediate))

			// output file
			oname := "mr-out-" + strconv.Itoa(reply.ReduceTaskNumber)
			ofile, _ := ioutil.TempFile("", oname+"*")

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			os.Rename(ofile.Name(), oname)
			ofile.Close()

			for i := 0; i < reply.NMap; i++ {
				iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNumber)
				err := os.Remove(iname)
				if err != nil {
					log.Fatalf("cannot open delete" + iname)
				}
			}

			// send the finish message to master
			finishedArgs := WorkerArgs{-1, reply.ReduceTaskNumber}
			finishedReply := WorkerReply{}
			call("Coordinator.ReceiveFinishedReduce", &finishedArgs, &finishedReply)
		}
		time.Sleep(time.Second)
	}
	return
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
