package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
)

// MapperInterface 需要客户定制的方法,读入信息之后进行映射
type MapperInterface func(node *Worker, reader io.Reader) *MapResult

// ReduceInterface 需要客户定制的方法,进行reduce操作，返回结果以及信息
type ReduceInterface func(node *Worker, reader io.Reader) *ReduceResult

// HandleTaskInterface 需要客户定制的方法,如何处理master发布的任务
type HandleTaskInterface func(node *Worker, task *Task, accept *bool) error

// WorkerInterface 基础节点需要实现的方法
type WorkerInterface interface {
	HandleTask(task *Task, accept *bool) error //处理master发布的任务
	Register() error                           //将自身注册至master
	pong() error                               //接受master的ping表示自己的状态
	Info(msg interface{}) error                //通知master最终的执行信息
}

// Worker 基础节点，提供mapreduce框架功能
type Worker struct {
	id                        int
	masterAddress, masterPort string //master的地址
	score                     uint32 //性能越高，越会被优先调度
	client                    *rpc.Client
	address, port             string   //自身的地址
	nodeType                  NodeType //节点的类型
	mapFunc                   MapperInterface
	reduceFunc                ReduceInterface
	handleTaskFunc            HandleTaskInterface
}

//暂未实现
func (worker *Worker) pong() error {
	//TODO implement me
	panic("implement me")
}

func (worker *Worker) HandleTask(task *Task, accept *bool) error {
	return worker.handleTaskFunc(worker, task, accept)
}

func (worker *Worker) Register() error {
	//将自身注册至master
	success := false
	err := worker.client.Call("Master.Register", worker.buildWorkInfo(), &success)
	if err == nil && success {
		return nil
	}
	return fmt.Errorf("register fail with %v", err)
}

func (worker *Worker) buildWorkInfo() *WorkerInfo {
	return &WorkerInfo{
		id:         worker.id,
		address:    worker.address,
		port:       worker.port,
		workerType: worker.nodeType,
		score:      worker.score,
	}
}

func (worker *Worker) Info(msg interface{}) (err error) {
	success := false
	if worker.nodeType == MAPPER {
		result, ok := msg.(*MapResult)
		if !ok {
			return fmt.Errorf("wrong data format when mapper submit")
		}
		err = worker.client.Call("Master.MapperSubmit", result, &success)
	} else {
		result, ok := msg.(*MapResult)
		if !ok {
			return fmt.Errorf("wrong data format when reducer submit")
		}
		err = worker.client.Call("Master.MapperSubmit", result, &success)
	}
	if err == nil && success {
		return nil
	}
	return fmt.Errorf("submit task result fail with %v", err)
}

func NewNode(address string, port string, nodeType NodeType, id int) *Worker {
	return &Worker{address: address, port: port, nodeType: nodeType, id: id}
}

func (worker *Worker) WithMasterConfig(masterAddr, masterPort string) error {
	worker.masterAddress = masterAddr
	worker.masterPort = masterPort
	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%s", masterAddr, masterPort))
	if err != nil {
		return err
	}
	worker.client = client
	return nil
}
func (worker *Worker) WithScore(score uint32) {
	worker.score = score
}

func (worker *Worker) WithMapperFunc(f MapperInterface) error {
	if worker.nodeType != MAPPER {
		return fmt.Errorf("Worker mismatch ")
	}
	if f == nil {
		return fmt.Errorf("MapperInterface is nil")
	}
	worker.mapFunc = f
	return nil
}

func (worker *Worker) WithReduceFunc(f ReduceInterface) error {
	if worker.nodeType != REDUCER {
		return fmt.Errorf("Worker mismatch ")
	}
	if f == nil {
		return fmt.Errorf("ReduceInterface is nil")
	}
	worker.reduceFunc = f
	return nil
}

func (worker *Worker) WithHandleTaskFunc(f HandleTaskInterface) error {
	if f == nil {
		return fmt.Errorf("HandleTaskInterface is nil")
	}
	worker.handleTaskFunc = f
	return nil
}
func (worker *Worker) Start() error {
	name := ""
	if worker.nodeType == MAPPER {
		name = "Mapper"
	} else {
		name = "Reducer"
	}
	err := rpc.RegisterName(name, worker)
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", worker.address, worker.port))
	if err != nil {
		log.Fatal("ListenTCP error:", err)
	}
	conn, err := listener.Accept()
	if err != nil {
		log.Fatal("Accept error:", err)
	}
	rpc.ServeConn(conn)
	return err
}
