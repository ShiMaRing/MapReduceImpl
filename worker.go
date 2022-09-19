package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

// MapperInterface 传入任务信息以及worker实例，用户需要返回每一个R对应的文件列表
type MapperInterface func(node *Worker, task *Task) (map[int][]string, error)

// ReduceInterface 传入任务信息以及worker实例，用户需要返回生成的文件列表
type ReduceInterface func(node *Worker, task *Task) ([]string, error)

// WorkerInterface 基础节点需要实现的方法
type WorkerInterface interface {
	HandleTask(task *Task, accept *bool) error      //处理master发布的任务
	Register() error                                //将自身注册至master
	pong() error                                    //接受master的ping表示自己的状态
	info(msg interface{}, errMsg error) (err error) //通知master最终的执行信息
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
}

//暂未实现
func (worker *Worker) pong() error {
	//TODO implement me
	panic("implement me")
}

// HandleTask 是否接受任务
func (worker *Worker) HandleTask(task *Task, accept *bool) error {

	if task.TaskType != worker.nodeType {
		*accept = false
		return fmt.Errorf("unreasonable assignment of tasks ")
	}
	if task.TaskType == MAPPER {
		if task.R == 0 {
			*accept = false
			return fmt.Errorf("R is 0 ")
		}

		if _, err := os.Stat(task.FileName); errors.Is(err, os.ErrNotExist) {
			*accept = false
			return fmt.Errorf("not found file ")
		}

		go func() {
			err := worker.info(worker.mapFunc(worker, task))
			log.Println(err)
		}()

	} else {
		if task.FilePaths == nil {
			*accept = false
			return fmt.Errorf("FilePaths is nil ")
		}
		go func() {
			err := worker.info(worker.reduceFunc(worker, task))
			log.Println(err)
		}()
	}

	*accept = true
	return nil
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
		Id:         worker.id,
		Address:    worker.address,
		Port:       worker.port,
		WorkerType: worker.nodeType,
		Score:      worker.score,
	}
}

func (worker *Worker) info(msg interface{}, errMsg error) (err error) {
	success := false
	//fmt.Println("worker", *worker)
	if worker.nodeType == MAPPER {
		result, ok := msg.(map[int][]string)
		if !ok {
			return fmt.Errorf("wrong data format when mapper submit")
		}

		mRes := &MapResult{
			MapperId: worker.id,
			Res:      result,
		}
		if errMsg != nil {
			mRes.State = FAILED
			mRes.ErrType = errMsg.Error()
		}
		err = worker.client.Call("Master.MapperSubmit", mRes, &success)
	} else {
		result, ok := msg.([]string)
		if !ok {
			return fmt.Errorf("wrong data format when reducer submit")
		}
		RRes := &ReduceResult{
			ReducerId: worker.id,
			Res:       result,
		}
		if errMsg != nil {
			RRes.State = FAILED
			RRes.ErrType = errMsg.Error()
		}
		err = worker.client.Call("Master.ReduceSubmit", RRes, &success)
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

func (worker *Worker) WithMapperFunc(f MapperInterface) *Worker {
	worker.mapFunc = f
	return worker
}

func (worker *Worker) WithReduceFunc(f ReduceInterface) *Worker {
	worker.reduceFunc = f
	return worker
}

func (worker *Worker) Start() error {

	name := ""
	if worker.nodeType == MAPPER {
		name = "Mapper-" + strconv.Itoa(worker.id)
	} else {
		name = "Reducer-" + strconv.Itoa(worker.id)
	}
	err := rpc.RegisterName(name, worker)

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", worker.address, worker.port))
	if err != nil {
		log.Fatal("ListenTCP error:", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
		}
		go rpc.ServeConn(conn)
	}

	return err
}
