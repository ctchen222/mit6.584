package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce         int   // number of reduce tasks
	nReduceFinished int   // number of finished reduce tasks
	reduceTaskLog   []int // log of finished reduce tasks
	nMap            int   // number of map tasks
	nMapFinished    int   // number of finished map tasks
	mapTaskLog      []int // log for map task, 0: not allocated, 1: allocated, 2: finished
	files           []string
	mu              sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReceiveFinishedMap(arg *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapTaskLog[arg.MapTaskNumber] = 2
	c.nMapFinished++
	return nil
}

func (c *Coordinator) ReceiveFinishedReduce(arg *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nMapFinished++
	c.reduceTaskLog[arg.ReduceTaskNumber] = 2
	return nil
}

func (c *Coordinator) AllocateTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	if c.nMapFinished < c.nMap {
		// allocate new map task
		allocate := -1
		for i := 0; i < c.nMap; i++ {
			if c.mapTaskLog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// waiting for unfinished map jobs
			reply.Tasktype = 2
			c.mu.Unlock()
		} else {
			// allocate map jobs
			reply.NReduce = c.nReduce
			reply.Tasktype = 0
			reply.MapTaskNumber = allocate
			reply.Filename = c.files[allocate]
			c.mapTaskLog[allocate] = 1 // waiting
			c.mu.Unlock()              // avoid deadlock
			go func() {
				time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
				c.mu.Lock()
				if c.mapTaskLog[allocate] == 1 {
					// still waiting, assume the map worker is died
					c.mapTaskLog[allocate] = 0
				}
				c.mu.Unlock()
			}()
		}
	} else if c.nMapFinished == c.nMap && c.nReduceFinished < c.nReduce {
		// allocate new reduce task
		allocate := -1
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskLog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// waiting for unfinished reduce jobs
			reply.Tasktype = 2
			c.mu.Unlock()
		} else {
			// allocate reduce jobs
			reply.NMap = c.nMap
			reply.Tasktype = 1
			reply.ReduceTaskNumber = allocate
			c.reduceTaskLog[allocate] = 1 // waiting
			c.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
				c.mu.Lock()
				if c.reduceTaskLog[allocate] == 1 {
					// still waiting, assume the reduce worker is died
					c.reduceTaskLog[allocate] = 0
				}
				c.mu.Unlock()
			}()
		}
	} else {
		reply.Tasktype = 3
		c.mu.Unlock()
	}
	return nil

}

func (c *Coordinator) Hello(args *WorkerArgs, reply *WorkerReply) error {
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.nReduce == c.nReduceFinished
	// ret := c.nReduce == c.nReduceFinished

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTaskLog = make([]int, c.nMap)
	c.reduceTaskLog = make([]int, c.nReduce)
	c.server()
	return &c
}
