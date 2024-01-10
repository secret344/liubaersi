package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	Type      TaskType
	MapId     int
	ReduceId  int
	File      string
	BeginTime time.Time
	Status    TaskStatus
}
type Coordinator struct {
	MapChannel      chan *Task // map任务
	ReduceChannel   chan *Task // reduce任务
	Files           []string   // 文件
	MapNum          int        // 任务数量
	ReduceNum       int
	DistributePhase DistributePhase //阶段
	Lock            sync.Mutex      //锁
	WorkerId        int             //workerid
	IsDone          bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.Lock.Lock()
	reply.Task = nil
	if c.DistributePhase == MapTask {
		for i := 0; i < c.MapNum; i++ {
			task := <-c.MapChannel
			c.MapChannel <- task
			if task.Status == Ready {
				task.MapId = c.WorkerId
				c.WorkerId += 1
				task.BeginTime = time.Now()
				task.Status = Running
				reply.Task = task
			}
		}
	} else {
		for i := 0; i < c.ReduceNum; i++ {
			task := <-c.ReduceChannel
			c.ReduceChannel <- task
			if task.Status == Ready {
				task.ReduceId = c.WorkerId
				c.WorkerId += 1
				task.BeginTime = time.Now()
				task.Status = Running
				reply.Task = task
			}
		}
	}
	c.Lock.Unlock()
	if reply.Task == nil {
		tip := "map"
		if c.DistributePhase == ReduceTask {
			tip = "reduce"
		}
		// 未查找到任务时，检查是否结束，清除临时文件 TODO
		return errors.New("No " + tip + " task available")
	} else {
		return nil
	}
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
	ret := false

	// Your code here.

	return ret
}

// 生成task任务 随后交给workder执行
func (c *Coordinator) generateMapTasks(files []string) {
	c.Lock.Lock()
	for _, file := range files {
		task := Task{
			Type:   MapTask,
			File:   file,
			Status: Ready,
		}
		c.MapChannel <- &task
		log.Println("Finis generating map task :", task)
	}
	c.Lock.Unlock()
	log.Println("Finis generating all map task :")
}

// 定期检查任务 删除过期的任务

func (c *Coordinator) periodicallyRmExpTasks() {

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Println("coordinator 创建")
	num := len(files)
	c := Coordinator{
		MapChannel:      make(chan *Task, num),
		ReduceChannel:   make(chan *Task, nReduce),
		Files:           files,
		MapNum:          num,
		ReduceNum:       nReduce,
		DistributePhase: MapPhase,
		WorkerId:        1,
	}

	c.generateMapTasks(files)

	c.server()
	return &c
}
