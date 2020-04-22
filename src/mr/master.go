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

func (m *Master) JobDispatch(args *MRArgs, reply *MRReply) error {
	// check the result of the last job
	m.handleJobResult(args, reply)
	// assign new job
	select {
	case num := <-m.mapJobChan:
		log.Printf("JobDispatch: Map job num %v", num)
		m.mu.Lock()
		defer m.mu.Unlock()
		reply.JobType = "MAP"
		reply.File = m.mapJobs[num]
		reply.MId = num
		reply.RId = -1
		reply.NReduce = m.nReduce
		reply.NMap = m.nMap
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		key := "MAP" + strconv.Itoa(num)
		m.jobContext[key] = cancel
		go m.handleContextTimeout(ctx, "MAP", num)
		return nil
	case num := <-m.reduceJobChan:
		log.Printf("JobDispatch: Reduce job num %v", num)
		m.mu.Lock()
		defer m.mu.Unlock()
		reply.JobType = "REDUCE"
		reply.MId = -1
		reply.RId = num
		reply.NReduce = m.nReduce
		reply.NMap = m.nMap
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		key := "REDUCE" + strconv.Itoa(num)
		if num >= 0 {
			m.jobContext[key] = cancel
			go m.handleContextTimeout(ctx, "REDUCE", num)
		} else {
			log.Print("Hey worker: all works are done!")
			reply.Status = "DONE"
		}
		return nil
	}
}

func (m *Master) handleContextTimeout(ctx context.Context, jobType string, jobId int) {
	select {
	case <-ctx.Done():
		if ctx.Err() != context.Canceled {
			log.Printf("Oops: Timeout! %v job %v redo", jobType, jobId)
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

func (m *Master) handleJobResult(args *MRArgs, reply *MRReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch args.Status {
	case "FINISHED":
		key := ""
		if args.JobType == "MAP" {
			log.Printf("%v job: %v is Finished, current nMap: %v", args.JobType, args.MId, m.nMap)
			key = "MAP" + strconv.Itoa(args.MId)
			m.nMap--
			if m.nMap == 0 {
				go func() {
					log.Println("Hey worker: Map jobs are done, let's start to reduce!")
					for i := 0; i < m.nReduce; i++ {
						m.reduceJobChan <- i
					}
				}()
			}
		} else {
			log.Printf("%v job: %v is Finished, current nReduce %v", args.JobType, args.RId, m.nReduce)
			key = "REDUCE" + strconv.Itoa(args.RId)
			m.nReduce--
			if m.nReduce == 0 {
				log.Println("Reduce jobs are done!")
				go func() {
					for i := 0; i < 2; i++ {
						m.reduceJobChan <- -1
					}
				}()
			}
		}
		if cancel, ok := m.jobContext[key]; ok {
			cancel()
			delete(m.jobContext, key)
		}
	case "FAILED":
		log.Printf("%v job: %v is FAILED", args.JobType, args.MId)
		key := ""
		if args.JobType == "MAP" {
			key = "MAP" + strconv.Itoa(args.MId)
			go func() {
				m.mapJobChan <- args.MId
			}()
		} else {
			key = "REDUCE" + strconv.Itoa(args.RId)
			go func() {
				m.reduceJobChan <- args.RId
			}()
		}
		if cancel, ok := m.jobContext[key]; ok {
			cancel()
			delete(m.jobContext, key)
		}
	}
	return nil
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
	ret = m.nMap <= 0 && m.nReduce <= 0
	log.Printf("DONE: nmap: %v nreduce %v", m.nMap, m.nReduce)
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mu:            sync.Mutex{},
		nReduce:       nReduce,
		nMap:          len(files),
		mapJobs:       make(map[int]string),
		mapJobChan:    make(chan int, len(files)),
		reduceJobChan: make(chan int, nReduce),
		jobContext:    make(map[string]context.CancelFunc),
	}

	// Your code here.
	// initial the master
	for index, file := range files {
		m.mapJobChan <- index
		m.mapJobs[index] = file
		log.Println(index, file)
	}

	log.Printf("nReduce %v, nMap: %v", m.nReduce, m.nMap)
	m.server()
	return &m
}
