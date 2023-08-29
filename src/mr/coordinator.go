package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	input                    []string
	nReduce                  int
	mapTaskIsComplete        []bool
	reduceTaskIsComplete     []bool
	remainingMapTasksCount   int
	remainingReduceTaskCount int
	tasksChannel             chan Task
	mapWg                    sync.WaitGroup
	currentPhase             Phase
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
	/*
		Initially assign MapTasks.
		When no MapsTasks are left but, few are yet to complete then assign WaitTask.
		Once all MapTasks done execiting then assign ReduceTasks.
		Once all ReduceTasks done executing assign TreminateTasks.
	*/

	log.Printf("Recived a request from worker process id %v \n", args.Pid)

	select {
	case task, ok := <-c.tasksChannel:
		if !ok {
			reply.Task = Task{
				TaskType: Terminate,
			}
			return nil
		}

		reply.Task = task
		reply.NReduce = c.nReduce
		return nil
	default:
		reply.Task = Task{
			Id:       -1,
			Input:    "",
			TaskType: Wait,
		}
		reply.NReduce = 0
		return nil
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.remainingMapTasksCount+c.remainingReduceTaskCount == 0
}

func (c *Coordinator) DoneMapTask(taskId int, reply *bool) error {
	if c.mapTaskIsComplete[taskId] {
		return nil
	} else if taskId >= len(c.mapTaskIsComplete) {
		return errors.New("index out of bound error")
	}

	log.Printf("Completed [%v] Map Task.\n", taskId)
	c.mapTaskIsComplete[taskId] = true
	c.remainingMapTasksCount -= 1
	c.mapWg.Done()
	*reply = true
	return nil
}

func (c *Coordinator) DoneReduceTask(taskId int, reply *bool) error {
	if c.reduceTaskIsComplete[taskId] {
		return nil
	} else if taskId >= len(c.reduceTaskIsComplete) {
		return errors.New("index out of bound error")
	}

	log.Printf("Completed [%v] Reduce Task.\n", taskId)
	c.reduceTaskIsComplete[taskId] = true
	c.remainingReduceTaskCount -= 1
	*reply = true
	return nil
}

func (c *Coordinator) InitMapTasks() {
	log.Println("Starting Map Phase")
	for index, file := range c.input {
		c.tasksChannel <- Task{
			Input:    file,
			Id:       index,
			TaskType: MapTask,
		}
	}
}

func (c *Coordinator) InitReduceTasks() {
	c.mapWg.Wait()
	c.currentPhase = ReducePhase
	log.Println("Starting Reduce Phase")

	for i := 0; i < c.nReduce; i++ {
		c.tasksChannel <- Task{
			Input:    strconv.Itoa(i),
			Id:       i,
			TaskType: ReduceTask,
		}
	}
	close(c.tasksChannel)
}

func (c *Coordinator) InitTasks() {
	go c.InitMapTasks()
	go c.InitReduceTasks()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		input:                    files,
		nReduce:                  nReduce,
		mapTaskIsComplete:        make([]bool, len(files)),
		reduceTaskIsComplete:     make([]bool, nReduce),
		remainingMapTasksCount:   len(files),
		remainingReduceTaskCount: nReduce,
		tasksChannel:             make(chan Task),
		currentPhase:             MapPhase,
	}

	// Your code here.
	c.mapWg.Add(len(files))
	c.InitTasks()

	c.server()
	return &c
}
