package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func AssignTaskCall() (AssignTaskResponse, error) {
	request := AssignTaskRequest{
		Pid: os.Getpid(),
	}
	response := AssignTaskResponse{}

	ok := call("Coordinator.AssignTask", &request, &response)
	if !ok {
		return response, errors.New("assign task rpc call failed")
	} else if response.Done {
		return response, errors.New("all task have been assigned")
	}

	return response, nil
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

func splitToBuckets(kva []KeyValue, numBuckets int) [][]KeyValue {
	buckets := make([][]KeyValue, numBuckets)

	for _, kv := range kva {
		buckets[ihash(kv.Key)%numBuckets] = append(buckets[ihash(kv.Key)%numBuckets], kv)
	}

	return buckets
}

func writeToIntermediateFiles(mapId int, index int, bucket []KeyValue) {
	interMediateFileName := "map" + "-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(index)
	file, err := os.Create(interMediateFileName)
	if err != nil {
		fmt.Printf("error creating file: %v \n", err.Error())
		return
	}

	encoder := json.NewEncoder(file)
	for _, kv := range bucket {
		err := encoder.Encode(kv)
		if err != nil {
			fmt.Printf("error writing to file: task id %v, key: %v, value %v, cause %v \n", mapId, kv.Key, kv.Value, err.Error())
			continue
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// TODO
	//  1. call coordinater and get the task to run
	// 2. run the task.
	// 3. save the key values in intermideate files with mr-X-Y.json format

	response, err := AssignTaskCall()
	if err != nil {
		log.Fatal(err)
		return
	}

	nReduce := response.NReduce
	task := response.Task
	fmt.Printf("Task id: [%v] filename: [%v] \n", task.Id, task.Filename)

	filepath := "../main/" + task.Filename
	content, err := os.ReadFile(filepath)
	if err != nil {
		fmt.Printf("error reading file: %v \n", err.Error())
		return
	}

	kva := mapf(task.Filename, string(content))
	fmt.Printf("intermediate key-value pairs %v \n", len(kva))

	buckets := splitToBuckets(kva, nReduce)
	var wg sync.WaitGroup

	for index, bucket := range buckets {
		index := index
		bucket := bucket
		wg.Add(1)

		go func() {
			defer wg.Done()
			writeToIntermediateFiles(task.Id, index, bucket)
		}()
	}

	wg.Wait()
	fmt.Printf("Done Map Task id: [%v]\n", task.Id)
}
