package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
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
	}

	return response, nil
}

func DoneMapTaskCall() error {
	request := true
	var response bool

	ok := call("Coordinator.DoneMapTask", &request, &response)
	if !ok {
		return errors.New("done map task rpc call failed")
	}

	return nil
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

	log.Println(err)
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
	interMediateFileName := "./data/" + "map" + "-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(index)
	file, err := os.Create(interMediateFileName)
	if err != nil {
		log.Printf("error creating file: %v \n", err.Error())
		return
	}

	encoder := json.NewEncoder(file)
	for _, kv := range bucket {
		err := encoder.Encode(kv)
		if err != nil {
			log.Printf("error writing to file: task id %v, key: %v, value %v, cause %v \n", mapId, kv.Key, kv.Value, err.Error())
			continue
		}
	}
}

func HandleMapTask(task Task, nReduce int, mapf func(string, string) []KeyValue) {
	log.Printf("Task id: [%v] filename: [%v] \n", task.Id, task.Input)
	filepath := "../main/" + task.Input
	content, err := os.ReadFile(filepath)
	if err != nil {
		log.Printf("error reading file: %v \n", err.Error())
		return
	}

	kva := mapf(task.Input, string(content))
	log.Printf("intermediate key-value pairs %v \n", len(kva))

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
	if err := DoneMapTaskCall(); err != nil {
		log.Printf("Error during rpc call: %v", err.Error())
	}
	log.Printf("Done Map Task id: [%v]\n", task.Id)
}

func GetFilesWithPattern(path string, regex *regexp.Regexp) ([]*os.File, error) {

	files, err := os.ReadDir(path)
	if err != nil {
		return nil, errors.New("error while reading dir " + err.Error())
	}

	fs := make([]*os.File, 0)
	for _, f := range files {
		if regex.MatchString(f.Name()) {
			file, err := os.Open(path + f.Name())
			if err != nil {
				return nil, errors.New("error while opening the file " + err.Error())
			}

			fs = append(fs, file)
		}
	}

	return fs, nil
}

func GetKeyValueArray(s string) ([]KeyValue, error) {
	pattern := "map-[0-9]+-" + s + "$"
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, errors.New(err.Error())
	}

	files, err := GetFilesWithPattern("./data/", regex)
	if err != nil {
		return nil, errors.New(err.Error())
	}
	log.Printf("Found %v files with %v pattern", files, pattern)

	var wg sync.WaitGroup
	kvChan := make(chan KeyValue)
	kva := make([]KeyValue, 0, 10)

	for _, file := range files {
		wg.Add(1)
		go func(file *os.File) {
			defer wg.Done()

			decoder := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := decoder.Decode(&kv); err != nil {
					break
				}
				kvChan <- kv
			}
		}(file)
	}

	go func() {
		wg.Wait()
		close(kvChan)
	}()

	for kv := range kvChan {
		kva = append(kva, kv)
	}
	return kva, nil
}

func buildMap(kva []KeyValue) map[string][]string {
	kvMap := make(map[string][]string)

	for _, kv := range kva {
		if _, ok := kvMap[kv.Key]; !ok {
			kvMap[kv.Key] = make([]string, 0)
		}

		kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
	}

	return kvMap
}

func HandleReduceTask(task Task, reducef func(string, []string) string) {
	/*
		1. read from file in data folder
		2. deserialize to go objects
		3. call reduce funtion on the objects
	*/
	kva, err := GetKeyValueArray(task.Input)
	if err != nil {
		log.Fatalln(err.Error())
	}

	kvMap := buildMap(kva) // kvMap := make(map[string][]string, 0)
	file, err := os.Create("./data/" + "mr" + "-" + "out" + "-" + strconv.Itoa(task.Id))
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	for key, value := range kvMap {
		reduced := reducef(key, value)
		fmt.Fprintf(file, "%v %v\n", key, reduced)
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
