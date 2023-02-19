package mr

import (
	"context"
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MAP = iota
	REDUCE
	WAIT
	NO
)

const (
	FREE = iota
	RUNNING
	FINISHED
)

type Task struct {
	Id     int64
	Type   int64
	Status int64
	Files  []string
	Ctx    context.Context
}
type Coordinator struct {
	Tasks    chan *Task
	Finished bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	return c.Finished
}

func (c *Coordinator) FinishTask(id int64) {

}

func (c *Coordinator) RequestForTask(t *Task) {
	select {
	case task := <-c.Tasks:
		t = task

		ctx, cancel := context.WithTimeout(t.Ctx, 10*time.Second)
		defer cancel()
		go func() {
			select {
			case <-ctx.Done():
				fmt.Printf("job %d 超时\n", t.Id)
			}
		}()
	case <-c.runningTasks:
		t = &Task{
			Type: WAIT,
		}
	default:
		t = &Task{
			Type: NO,
		}
	}

}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, filename := range files {
		c.Tasks <- &Task{
			Files:  []string{filename},
			Type:   MAP,
			Status: FREE,
			Ctx:    context.Background(),
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce Tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Tasks = make(chan *Task, 100)
	c.runningTasks = make(chan *Task, 100)
	c.makeMapTasks(files)

	c.server()
	return &c
}
