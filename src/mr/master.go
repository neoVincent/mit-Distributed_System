package mr

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mu            sync.Mutex
	nReduce       int            // job progress indicator
	nMap          int            // job progress indicator
	mapJobs       map[int]string // map id --> file name
	mapJobChan    chan int
	reduceJobChan chan int
	jobContext    map[string]context.CancelFunc
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) JobDispatch(arg *MRArgs, reply *MRReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// check the result of the last job
	switch arg.Status {
	case "FINISHED":
		key := ""
		if arg.JobType == "MAP" {
			m.nMap--
			key = "MAP" + strconv.Itoa(arg.MId)
		} else {
			m.nReduce--
			key = "REDUCE" + strconv.Itoa(arg.RId)
		}
		cancel := m.jobContext[key]
		cancel()
		delete(m.jobContext, key)
	case "FAILED":
		key := ""
		if arg.JobType == "MAP" {
			key = "MAP" + strconv.Itoa(arg.MId)
			m.mapJobChan <- arg.MId
		} else {
			key = "REDUCE" + strconv.Itoa(arg.RId)
			m.reduceJobChan <- arg.RId
		}
		cancel := m.jobContext[key]
		cancel()
		delete(m.jobContext, key)
	}

	// assign new job
	select {
	case num := <-m.mapJobChan:
		reply.JobType = "MAP"
		reply.File = m.mapJobs[num]
		reply.MId = num
		reply.RId = -1
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		key := "MAP" + strconv.Itoa(num)
		m.jobContext[key] = cancel
		go m.handleContextTimeout(ctx, "MAP", num)
		return nil
	case num := <-m.reduceJobChan:
		// TODO: ADD REDUCE LOGIC
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		key := "REDUCE" + strconv.Itoa(num)
		m.jobContext[key] = cancel
		go m.handleContextTimeout(ctx, "REDUCE", num)
		return nil
	}
}

func (m *Master) handleContextTimeout(ctx context.Context, jobType string, jobId int) {
	select {
	case <-ctx.Done():
		if ctx.Err() != context.Canceled {
			m.mu.Lock()
			defer m.mu.Unlock()
			if jobType == "MAP" {
				m.mapJobChan <- jobId
			} else {
				m.reduceJobChan <- jobId
			}
		}
	}
}

//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	ret = m.nMap == 0 && m.nReduce == 0
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	// initial the master
	// TODO: initialize the master

	m.server()
	return &m
}
