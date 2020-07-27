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

type Task struct {
	number   int
	workerId int
	started  time.Time
}

type Master struct {
	mu sync.Mutex

	nMap    int
	nReduce int

	isMapPhase    bool
	isReducePhase bool
	isDone        bool

	mapTasks       map[int]string
	availableTasks []int
	currentTasks   map[int]Task
}

// RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) HandleTaskRequest(req *TaskRequest, reply *TaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isDone && len(m.availableTasks) > 0 {
		var task int
		task, m.availableTasks = m.availableTasks[0], m.availableTasks[1:]
		m.currentTasks[task] = Task{number: task, workerId: req.WorkerId, started: time.Now()}

		if m.isMapPhase {
			reply.IsMapTask = true
			reply.IsReduceTask = false
			reply.Filename = m.mapTasks[task]
		} else if m.isReducePhase {
			reply.IsMapTask = false
			reply.IsReduceTask = true
		}
		reply.NReduce = m.nReduce
		reply.NMap = m.nMap
		reply.TaskNumber = task

		// log.Printf("Worker #%d asked for task, sending %v\n", req.WorkerId, reply)
	}

	return nil
}

func (m *Master) HandleTaskComplete(task *TaskComplete, reply *TaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// worker was removed due to timeout
	if _, present := m.currentTasks[task.TaskNumber]; !present {
		log.Panicf("Task #%d not present in current tasks!", task.TaskNumber)
		return nil
	}

	// received from removed worker due to a timeout
	if m.currentTasks[task.TaskNumber].workerId != task.WorkerId {
		log.Printf("Complete message for task #%d received from unexpected worker #%d", task.TaskNumber, task.WorkerId)
		// return nil
	}

	log.Printf("Worker #%d completed the task #%d\n", task.WorkerId, task.TaskNumber)

	// remove the task
	delete(m.currentTasks, task.TaskNumber)

	// check if map phase is done and refill available tasks if so
	m.isReducePhase = m.isReducePhase || len(m.currentTasks) == 0 && len(m.availableTasks) == 0
	if m.isReducePhase && m.isMapPhase {
		m.availableTasks = make([]int, m.nReduce)
		for i := 0; i < m.nReduce; i++ {
			m.availableTasks[i] = i
		}
		m.isMapPhase = false
	}

	// check if we are done for reduce
	m.isDone = m.isReducePhase && len(m.currentTasks) == 0 && len(m.availableTasks) == 0

	return nil
}

// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// check for timeouts
	for key, t := range m.currentTasks {
		elapsed := time.Since(t.started)
		if elapsed.Seconds() > 10 {
			log.Printf("Restarting task #%d...\n", key)
			delete(m.currentTasks, key)
			m.availableTasks = append(m.availableTasks, key)
		}
	}

	return m.isDone
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	mapTasks := make(map[int]string)
	tasks := make([]int, len(files))
	for i, file := range files {
		mapTasks[i] = file
		tasks[i] = i
	}

	m := Master{
		nMap:           len(files),
		nReduce:        nReduce,
		isMapPhase:     true,
		mapTasks:       mapTasks,
		availableTasks: tasks,
		currentTasks:   map[int]Task{},
	}
	m.server()
	return &m
}
