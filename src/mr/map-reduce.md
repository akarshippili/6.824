## Problem Statement
### 1. Your job is to implement a distributed MapReduce, consisting of two programs, the coordinator and the worker. 

### 2. There will be just one coordinator process, and one or more worker processes executing in parallel. 

### 3. In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine. 

### 4. The workers will talk to the coordinator via RPC. Each worker process will ask the coordinator for a task, read the task's input from one or more files, execute the task, and write the task's output to one or more files. 

### 5. The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.


## Idea

Coordinator

```
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
	reduceWg                 sync.WaitGroup
	currentPhase             Phase
}
```

RPC Handlers
```
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
```

```
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
```

```
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
```

```
func (c *Coordinator) DoneReduceTask(taskId int, reply *bool) error {
	if c.reduceTaskIsComplete[taskId] {
		return nil
	} else if taskId >= len(c.reduceTaskIsComplete) {
		return errors.New("index out of bound error")
	}

	log.Printf("Completed [%v] Reduce Task.\n", taskId)
	c.reduceTaskIsComplete[taskId] = true
	c.remainingReduceTaskCount -= 1
	c.reduceWg.Done()
	*reply = true
	return nil
}
```

```
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
```

```
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
```

```
func (c *Coordinator) InitTasks() {
	go c.InitMapTasks()
	go c.InitReduceTasks()
	go func() {
		c.mapWg.Wait()
		c.reduceWg.Wait()
		close(c.tasksChannel)
	}()
}
```

```
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
```

Worker
```
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// TODO
	//  1. call coordinater and get the task to run
	// 2. run the task.
	// 3. save the key values in intermideate files with mr-X-Y.json format
	for {
		response, err := AssignTaskCall()
		if err != nil {
			log.Fatal(err)
			return
		}

		nReduce := response.NReduce
		task := response.Task

		switch task.TaskType {
		case Wait:
			time.Sleep(time.Second)
			Worker(mapf, reducef)
			return
		case Terminate:
			os.Exit(0)
		case MapTask:
			HandleMapTask(task, nReduce, mapf)
		case ReduceTask:
			HandleReduceTask(task, reducef)
		}
	}
}
```

Common type
```
type TaskType int
type Phase int

type Task struct {
	Input    string
	Id       int
	TaskType TaskType
}

const (
	MapTask TaskType = iota
	ReduceTask
	Wait
	Terminate
)

const (
	MapPhase Phase = iota
	ReducePhase
)
```