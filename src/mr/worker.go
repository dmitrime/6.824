package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	pid := os.Getpid()
	quit := false

	for !quit {
		// Ask master for work
		reply, quit := SendTaskRequest(pid)
		if quit {
			// Calling master failed, so we exit.
			log.Fatal("Cannot contact master")
			break
		}

		if reply.IsMapTask {
			// got new map task
			doMap(mapf, reply.Filename, reply.TaskNumber, reply.NReduce)
			quit = SendTaskComplete(pid, reply.TaskNumber)
		} else if reply.IsReduceTask {
			// got new reduce task
			doReduce(reducef, reply.TaskNumber, reply.NMap)
			quit = SendTaskComplete(pid, reply.TaskNumber)
		} else {
			// no tasks available, let's wait
			time.Sleep(100 * time.Millisecond)
		}
	}

	// master was unreachable, terminate
	log.Printf("Terminating worker %d...", pid)
}

func doReduce(reducef func(string, []string) string, taskNumber int, nMap int) {
	kva := readMapOutput(taskNumber, nMap)

	oname := fmt.Sprintf("mr-out-%d", taskNumber)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in kva[] and print the result to mr-out-taskNumeber.
	i := 0
	count := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
		count++
	}
	log.Printf("Found %d keys for task %d\n", count, taskNumber)

	ofile.Close()
}

func readMapOutput(taskNumber int, nMap int) []KeyValue {
	var kva []KeyValue

	for n := 0; n < nMap; n++ {
		filename := fmt.Sprintf("mr-%d-%d", n, taskNumber)
		infile, err := os.Open(filename)
		if err != nil {
			log.Fatalf("File called '%s' not found during reduce task", filename)
		}

		dec := json.NewDecoder(infile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		infile.Close()
	}

	sort.Sort(ByKey(kva))

	return kva
}

func doMap(mapf func(string, string) []KeyValue, inputChunk string, taskNumber int, reducePieces int) {
	// Map the response filename using client function
	kva := mapFile(inputChunk, mapf)

	// Sort by keys
	sort.Sort(ByKey(kva))

	// Write intermediate file
	writeIntemediate(kva, taskNumber, reducePieces)
}

func writeIntemediate(kva []KeyValue, taskNumber int, nReduce int) {
	outName := fmt.Sprintf("mr-%d", taskNumber)

	var tmpFiles []*os.File
	var encs []*json.Encoder
	for n := 0; n < nReduce; n++ {
		tmpFile, err := ioutil.TempFile(os.TempDir(), outName)
		if err != nil {
			log.Fatal("Cannot create temporary file", err)
		}
		tmpFiles = append(tmpFiles, tmpFile)
		encs = append(encs, json.NewEncoder(tmpFile))
	}

	for _, kv := range kva {
		h := ihash(kv.Key) % nReduce
		err := encs[h].Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Close the temp files
	for _, tmp := range tmpFiles {
		if err := tmp.Close(); err != nil {
			log.Fatal(err)
		}
	}

	// Rename the temp files
	for n, tmp := range tmpFiles {
		if err := os.Rename(tmp.Name(), fmt.Sprintf("%s-%d", outName, n)); err != nil {
			log.Fatal(err)
		}
	}
}

func mapFile(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return mapf(filename, string(content))
}

//
// the RPC argument and reply types are defined in rpc.go.
//
func SendTaskRequest(pid int) (TaskResponse, bool) {
	//log.Printf("Worker %d asking for a new task...", pid)

	// declare an argument structure.
	args := TaskRequest{WorkerId: pid}

	// declare a reply structure.
	reply := TaskResponse{}

	// send the RPC request, wait for the reply.
	quit := call("Master.HandleTaskRequest", &args, &reply)

	return reply, !quit
}

func SendTaskComplete(pid int, taskNumber int) bool {
	args := TaskComplete{WorkerId: pid, TaskNumber: taskNumber}
	reply := TaskResponse{}
	return !call("Master.HandleTaskComplete", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
