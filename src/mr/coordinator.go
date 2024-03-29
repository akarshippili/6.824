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
	"time"
)

type Coordinator struct {
	// Your definitions here.
	input                []string
	nReduce              int
	mapTaskIsComplete    []bool
	reduceTaskIsComplete []bool
	tasksChannel         chan Task
	mapWg                sync.WaitGroup
	reduceWg             sync.WaitGroup
	currentPhase         Phase
	done                 chan bool
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
		go c.HandleCrash(task)
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
	select {
	case done := <-c.done:
		return done
	default:
		return false
	}
}

func (c *Coordinator) DoneMapTask(taskId int, reply *bool) error {
	if c.mapTaskIsComplete[taskId] {
		return nil
	} else if taskId >= len(c.mapTaskIsComplete) {
		return errors.New("index out of bound error")
	}

	log.Printf("Completed [%v] Map Task.\n", taskId)
	c.mapTaskIsComplete[taskId] = true
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
	c.reduceWg.Done()
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
	//  wait till all map tasks are done
	c.mapWg.Wait()

	// init reduce tasks
	c.currentPhase = ReducePhase
	log.Println("Starting Reduce Phase")

	for i := 0; i < c.nReduce; i++ {
		c.tasksChannel <- Task{
			Input:    strconv.Itoa(i),
			Id:       i,
			TaskType: ReduceTask,
		}
	}
}

func (c *Coordinator) InitTasks() {
	go c.InitMapTasks()
	go c.InitReduceTasks()
}

func (c *Coordinator) HandleCrash(task Task) {
	time.Sleep(10 * time.Second)

	taskId := task.Id
	if task.TaskType == MapTask && taskId < len(c.mapTaskIsComplete) && c.mapTaskIsComplete[taskId] {
		return
	} else if task.TaskType == ReduceTask && taskId < len(c.reduceTaskIsComplete) && c.reduceTaskIsComplete[taskId] {
		return
	}

	// worker crashed. handle it.
	// add the task back to Task chan
	c.tasksChannel <- task
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		input:                files,
		nReduce:              nReduce,
		mapTaskIsComplete:    make([]bool, len(files)),
		reduceTaskIsComplete: make([]bool, nReduce),
		tasksChannel:         make(chan Task),
		currentPhase:         MapPhase,
		done:                 make(chan bool, 1),
	}

	// Your code here.
	c.mapWg.Add(len(files))
	c.reduceWg.Add(nReduce)
	c.InitTasks()
	go func() {
		c.mapWg.Wait()
		c.reduceWg.Wait()
		close(c.tasksChannel)
		c.done <- true
	}()

	c.server()
	return &c
}
