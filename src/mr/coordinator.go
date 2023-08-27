package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	input        []string
	nReduce      int
	completed    []bool
	tasksChannel chan Task
	mapWg        sync.WaitGroup
	currentPhase Phase
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
	log.Printf("Recived a request from worker process id %v \n", args.Pid)
	task, ok := <-c.tasksChannel

	if !ok {
		reply.Task = Task{
			Id:       -1,
			Input:    "",
			TaskType: Wait,
		}
		reply.NReduce = 0
		return nil
	}

	reply.Task = task
	reply.NReduce = c.nReduce
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) DoneMapTask(args bool, reply *bool) error {
	c.mapWg.Done()
	*reply = true
	return nil
}

func (c *Coordinator) InitMapTasks() {
	for index, file := range c.input {
		c.mapWg.Add(1)
		c.tasksChannel <- Task{
			Input:    file,
			Id:       index,
			TaskType: MapTask,
		}
	}
	close(c.tasksChannel)
}

func (c *Coordinator) InitReduceTasks() {
	c.mapWg.Wait()
	c.currentPhase = ReducePhase
	log.Println("Done Waiting All Map task to complete")
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
		currentPhase: MapPhase,
	}

	// Your code here.

	go c.InitMapTasks()
	go c.InitReduceTasks()

	c.server()
	return &c
}
