package mr

import (
	"bufio"
	"errors"
	"fmt"
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
	stage             int
	nDistributeMap    int
	nUnFinishedMap    int
	nDistributeReduce int
	nUnFinishedReduce int
	nMap              int
	nReduce           int
}

var coordinatorMutex sync.Mutex

// Your code here -- RPC handlers for the worker to call.
// @brief 切分所有input为num_mapper个输入文件
// @param files
// @param num_mapper
// @return error
func split(files []string) (int, error) {
	num_mapper := 0
	size_sum := 0
	for _, file := range files {
		fileInfo, err := os.Stat(file)
		if err != nil {
			log.Fatal(err)
			return -1, err
		}
		size_sum += int(fileInfo.Size())
		fmt.Printf("%v %v\n", file, size_sum)
	}
	num_mapper = (size_sum)/262144 + 1
	if num_mapper <= 0 {
		return -1, errors.New("num_mapper error")
	}

	// create scanners
	scanners := make([]*bufio.Scanner, len(files))
	for index, file := range files {
		fd, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
			return -1, err
		}
		scanner := bufio.NewScanner(fd)
		scanners[index] = scanner
	}

	// open outout_file
	outers := make([]*os.File, num_mapper)
	for i := 0; i < num_mapper; i++ {
		file, err := os.OpenFile(
			"../mr-tmp/split-"+strconv.Itoa(i+1),
			os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
			0666,
		)
		if err != nil {
			log.Fatal(err)
			return -1, err
		}
		outers[i] = file
	}

	//
	index_output_file := 0
	for {
		split_end := true
		for _, scanner := range scanners {
			if scanner.Scan() {
				split_end = false
				outers[index_output_file].Write(scanner.Bytes())
				outers[index_output_file].Write([]byte("\n"))
				index_output_file++
				index_output_file %= len(outers)
			}
		}
		if split_end {
			break
		}
	}

	// close input&output fd
	for _, fd := range outers {
		fd.Close()
	}

	return num_mapper, nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Handle(request *CoordinatorRequest, response *CoordinatorResponse) error {
	coordinatorMutex.Lock()
	defer coordinatorMutex.Unlock()
	response.NMap = c.nMap
	response.NReduce = c.nReduce

	switch request.Action {
	case 0:
		if c.nDistributeMap > 0 && c.stage == 1 {
			response.Id = c.nDistributeMap
			response.Stage = c.stage
			c.nDistributeMap -= 1
		} else if c.nDistributeReduce > 0 && c.stage == 2 {
			response.Id = c.nDistributeReduce
			response.Stage = c.stage
			c.nDistributeReduce -= 1
		} else {
			response.Id = 0
			response.Stage = c.stage
			return errors.New("")
		}

	case 1:
		if c.stage == 1 {
			c.nUnFinishedMap -= 1
			if c.nUnFinishedMap == 0 {
				c.stage = 2
			}
		} else if c.stage == 2 {
			c.nUnFinishedReduce -= 1
			if c.nUnFinishedReduce == 0 {
				c.stage = 3
			}
		} else {
			response.Id = -1
			response.Stage = c.stage
		}

	default:
		response.Id = -1
		response.Stage = c.stage
		return errors.New("Error Action")
	}

	// fmt.Printf("[master] [Action = %v][stage = %v] [nDistributeMap = %v] [nUnFinishedMap = %v] [nDistributeReduce = %v] [nUnFinishedReduce = %v]\n",
	// 	request.Action, c.stage, c.nDistributeMap, c.nUnFinishedMap, c.nDistributeReduce, c.nUnFinishedReduce)
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	coordinatorMutex.Lock()
	defer coordinatorMutex.Unlock()
	if c.stage != 3 {
		return false
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nDistributeReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nDistributeReduce int) *Coordinator {

	// Your code here.
	// split

	nDistributeMap, err := split(files)
	fmt.Printf("nDistributeMap : %v", nDistributeMap)
	if err != nil {
		log.Fatal("split error: ", err)
		return nil
	}
	// init Coordinator
	c := Coordinator{
		1,
		nDistributeMap,
		nDistributeMap,
		nDistributeReduce,
		nDistributeReduce,
		nDistributeMap,
		nDistributeReduce,
	}
	//map_stage

	c.server()
	return &c
}
