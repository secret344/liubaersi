package mr

import (
	"hash/fnv"
	"log"
	"net/rpc"
)

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerInfo struct {
	id      int
	Mapf    MapFunc
	Reducef ReduceFunc
	Task    *Task
	IsDone  bool
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
	// CallExample()

}

func (worker *WorkerInfo) work() {
	for !worker.IsDone {
		task, err := worker.requestTask()
		if err != nil {
			log.Fatal("work.requestTask err = ", err)
			continue
		}
		if task == nil {
			log.Printf("Worker with WorkerId = %d receiver all task done", worker.id)
			worker.IsDone = true
			break
		}
		worker.Task = task
		err = nil
		tip := ""
		if worker.Task.Type == MapTask {
			// map任务执行
			worker.id = worker.Task.MapId
			err = worker.doMap()
			tip = "doMap"
		} else {
			worker.id = worker.Task.ReduceId
			err = worker.doReduce()
			tip = "doReduce"
		}
		if err != nil {
			log.Fatalf("Worker.%s err = %s", tip, err)
		}
		log.Printf("%s task done,  tasl = %#v", tip, task)
	}
}

// Map 任务执行
func (worker *WorkerInfo) doMap() error {
	// 执行文件读取，进行map统计

	// 统计结果写入临时文件

	// 返回临时文件信息 任务结束
	return nil
}

// Reduce 任务执行
func (worker *WorkerInfo) doReduce() error {
	return nil
}

func (worker *WorkerInfo) requestTask() (*Task, error) {
	// declare an argument structure.
	args := TaskArgs{}
	reply := TaskReply{}

	err := call("Coordinator.AssignTask", &args, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Task, nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}
	return nil
}
