package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	input        []string
	nReduce      int
	completed    []bool
	tasksChannel chan Task
}

type Task struct {
	Filename string
	Id       int
}

// Your code here -- RPC handlers for the worker to call.

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

func (c *Coordinator) AssignTask(args *AssignTaskRequest, reply *AssignTaskResponse) error {
	fmt.Printf("Recived a request from worker process id %v \n", args.Pid)
	task, ok := <-c.tasksChannel

	if !ok {
		reply.Task = Task{}
		reply.Done = true
		return nil
	}

	reply.Task = task
	reply.Done = false
	return nil
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
		input:        files,
		nReduce:      nReduce,
		completed:    make([]bool, len(files)),
		tasksChannel: make(chan Task),
	}

	// Your code here.

	go func() {
		for index, file := range files {
			c.tasksChannel <- Task{
				Filename: file,
				Id:       index,
			}
		}

		close(c.tasksChannel)
	}()

	c.server()
	return &c
}
