package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	nReduce           int
	inputFile         []string
	mapTasks          []Task
	mapTasksDone      int
	reduceTasks       []Task
	reduceTasksDone   int
	availableWokerIds []int
	intermediateFiles []string
	lock              sync.Mutex
}

func (c *Coordinator) AssignTask(args *RequestTask, replay *Task) error {
	var task *Task
	if c.mapTasksDone < len(c.inputFile) {
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == "idle" {
				task = &c.mapTasks[i]
				break
			}
		}
	} else if c.reduceTasksDone < c.nReduce {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == "idle" {
				task = &c.mapTasks[i]
				break
			}
		}
	}

	if task == nil {
		return errors.New("no idle task found")
	}

	replay.Type = task.Type
	replay.Id = task.Id
	task.Status = "inProgres"
	task.Time = time.Now()
	id := task.Id
	fmt.Printf("id: %d, taskId: %d, status: %s", id, c.mapTasks[id].Id, c.mapTasks[id].Status)

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFile:         files,
		nReduce:           nReduce,
		mapTasks:          make([]Task, len(files)),
		mapTasksDone:      0,
		reduceTasks:       make([]Task, nReduce),
		reduceTasksDone:   0,
		availableWokerIds: make([]int, 0),
		intermediateFiles: make([]string, 0),
		lock:              sync.Mutex{},
	}

	for i := range c.mapTasks {
		c.mapTasks[i] = Task{
			Id:     i,
			Type:   "map",
			Status: "idle",
			Time:   time.Now(),
		}
	}

	for i := range c.reduceTasks {
		c.reduceTasks[i] = Task{
			Id:     i,
			Type:   "reduce",
			Status: "idle",
			Time:   time.Now(),
		}
	}

	fmt.Printf("nMapTask = %d, nReduseTask = %d/n", len(c.mapTasks), len(c.reduceTasks))

	c.server()
	return &c
}
