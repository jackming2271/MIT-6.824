package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for true {
		response := RequestTask()
		// fmt.Printf("response.Stage = %v\n", response.Stage)
		if (response.Stage != 3) && (response.Id <= 0) {
			time.Sleep(1000 * time.Millisecond)
			continue
		} else if response.Stage == 3 {
			return
		}

		switch response.Stage {
		case 1:
			// open outout_file
			outers := make([]*os.File, response.NReduce)
			for i := 0; i < response.NReduce; i++ {
				file, err := os.OpenFile(
					"../mr-tmp/mr-"+strconv.Itoa(response.Id)+"-"+strconv.Itoa(i+1),
					os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
					0666,
				)
				if err != nil {
					log.Fatal(err)
					return
				}
				outers[i] = file
			}
			// 读入并统计
			inputFilePath := "../mr-tmp/split-" + strconv.Itoa(response.Id)
			fd, err := os.Open(inputFilePath)
			if err != nil {
				log.Fatal(err)
			}
			// scanner := bufio.NewScanner(fd)
			content, err := ioutil.ReadAll(fd)
			if err != nil {
				log.Fatalf("cannot read %v", fd)
			}
			// fmt.Printf("content :  %v", string(content))
			kva := mapf("filename", string(content))
			// fmt.Printf("kva: %v\n", kva)
			wordsMap := make(map[string]int)
			for i := 0; i < len(kva); i++ {
				// index := ihash(keyValues[i].Key) % response.NReduce
				wordsMap[kva[i].Key] += 1
				// outers[index].Write()
			}
			// 写入中间文件
			for key, value := range wordsMap {
				index := ihash(key) % response.NReduce
				outers[index].Write([]byte(key + " " + strconv.Itoa(value) + "\n"))
			}
			// 回复success 给master
			ResponseTask(response.Id)
		case 2:
			// 汇聚
			wordsMap := make(map[string]int)
			for i := 0; i < response.NMap; i++ {
				inputFilePath := "../mr-tmp/mr-" + strconv.Itoa(i+1) + "-" + strconv.Itoa(response.Id)
				fd, err := os.Open(inputFilePath)
				if err != nil {
					log.Fatal(err)
				}
				scanner := bufio.NewScanner(fd)
				for scanner.Scan() {
					key := strings.Split(scanner.Text(), " ")[0]
					value := strings.Split(scanner.Text(), " ")[1]
					num, _ := strconv.Atoi(value)
					wordsMap[key] += num
				}
			}
			// 写输出文件
			onputFilePath := "../mr-tmp/mr-out-" + strconv.Itoa(response.Id)
			file, err := os.OpenFile(
				onputFilePath,
				os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
				0666,
			)
			if err != nil {
				log.Fatal("error open output_reduce_file")
			}
			for key, value := range wordsMap {
				file.Write([]byte(key + " " + strconv.Itoa(value) + "\n"))
			}
			// 回复success 给master
			ResponseTask(response.Id)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	call("Coordinator.Example", &args, &reply)
}

func RequestTask() CoordinatorResponse {

	// declare an argument structure.
	request := CoordinatorRequest{}
	request.Action = 0
	// fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	response := CoordinatorResponse{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	call("Coordinator.Handle", &request, &response)
	return response
}

func ResponseTask(id int) {
	request := CoordinatorRequest{}
	request.Action = 1
	request.Id = id

	response := CoordinatorResponse{}

	ok := call("Coordinator.Handle", &request, &response)
	if ok {
		fmt.Printf("response.stage %v, response.id %v\n", response.Stage, response.Id)
	} else {
		fmt.Printf("call failed!\n")
	}
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
