package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

type MRJob struct {
	JobNum   int
	FileName string
	nReduce  int
	nMap     int
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// This is much like bucket sort
// use hash function to has all the key to different buckets
//
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

	//Your worker implementation here.
	mJobChan := make(chan MRJob)
	rJobChan := make(chan MRJob)
	ctx, cancel := context.WithCancel(context.Background()) // used to manage the MR Job
	args := MRArgs{
		Status: "INITIAL",
	}

	go requestJob(cancel, args, mJobChan, rJobChan)

	for {
		select {
		case mJob := <-mJobChan:
			err := doMap(mapf, mJob)
			if err != nil {
				args.Status = "FAILED"
			} else {
				args.Status = "FINISHED"
			}
			args.MId = mJob.JobNum
			args.JobType = "MAP"
			log.Printf("MAP: %v, request Job", args.Status)
			go requestJob(cancel, args, mJobChan, rJobChan)
		case rJob := <-rJobChan:
			err := doReduce(reducef, rJob)
			if err != nil {
				args.Status = "FAILED"
			} else {
				args.Status = "FINISHED"
			}
			args.RId = rJob.JobNum
			args.JobType = "REDUCE"
			log.Printf("REDUCE: %v, request Job", args.Status)
			go requestJob(cancel, args, mJobChan, rJobChan)
		case <-ctx.Done():
			log.Println("Worker is stopped")
			return
		}
	}

	// uncomment to send the Example RPC to the master.
	//CallExample()
}

func doMap(mapf func(string, string) []KeyValue,
	mJob MRJob) error {
	// read file
	// create kv structure by mapf
	log.Printf("Handle MAP job, job id: %v, file name: %v\n", mJob.JobNum, mJob.FileName)
	file, err := os.Open(mJob.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", mJob.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mJob.FileName)
	}
	if err := file.Close(); err != nil {
		log.Fatal(err)
	}
	kva := mapf(mJob.FileName, string(content))

	// Shuffle: use ihash func to get mr-X-Y
	for _, element := range kva {
		rid := ihash(element.Key) % mJob.nReduce
		oname := fmt.Sprintf("mr-%v-%v", mJob.JobNum, rid)
		ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println(err.Error())
		}
		enc := json.NewEncoder(ofile)
		if err := enc.Encode(element); err != nil {
			log.Println(err.Error())
		}
		if err := ofile.Close(); err != nil {
			log.Fatal(err)
		}

	}

	return nil
}

func doReduce(reducef func(string, []string) string,
	rJob MRJob) error {
	log.Printf("Handle REDUCE job, job id: %v", rJob.JobNum)
	// read file mr-*-Y (*: map , Y: reduce)
	pattern := fmt.Sprintf("mr-*-%v", rJob.JobNum)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Println(err)
	}
	dat := make(map[string][]string)
	for _, file := range matches {
		if strings.HasPrefix(file, "mr-out") {
			continue
		}
		log.Printf("doReduce: Read file %v", file)
		ifile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			log.Println(err.Error())
		}
		dec := json.NewDecoder(ifile)
		// iterate the file
		log.Printf("doReduce: Start to parse")
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			dat[kv.Key] = append(dat[kv.Key], kv.Value)
		}
		log.Printf("doReduce: Finsih parse")
		if err := ifile.Close(); err != nil {
			log.Fatal(err)
		}
	}

	// save into mr-out-X (X:reduce)
	ofileName := fmt.Sprintf("mr-out-%v", rJob.JobNum)
	ofile, _ := os.Create(ofileName)
	log.Printf("len(data) %v", len(dat))
	// TODO: for the multi reduce the loop becomes slow
	for key, val := range dat {
		count := reducef(key, val)
		if _, err := fmt.Fprintf(ofile, "%v %v\n", key, count); err != nil {
			log.Printf("doReduce save file error: %v", err.Error())
		}
	}
	log.Printf("Save to file %v", ofileName)
	ofile.Close()
	return nil
}

func requestJob(cancel context.CancelFunc, args MRArgs, mJobChan chan MRJob, rJobChan chan MRJob) {
	reply := MRReply{}
	call("Master.JobDispatch", &args, &reply)
	log.Printf("GET RESPONSE: Jobtype: %v Status: %v RID: %v MID %v", reply.JobType, reply.Status, reply.RId, reply.MId)

	if reply.Status == "DONE" || (reply.RId < 0 && reply.MId < 0) {
		log.Printf("All works are done!")
		cancel()
		return
	}

	// TODO: some cases the reply.JobType is empty in the test
	if reply.JobType == "MAP" {
		mJobChan <- MRJob{FileName: reply.File, JobNum: reply.MId, nMap: reply.NMap, nReduce: reply.NReduce}
	} else {
		rJobChan <- MRJob{FileName: reply.File, JobNum: reply.RId, nMap: reply.NMap, nReduce: reply.NReduce}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
