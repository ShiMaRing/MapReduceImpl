package main

import (
	"fmt"
	"io"
	"net/rpc"
)

// MapperInterface 需要客户定制的方法,读入信息之后进行映射
type MapperInterface func(node *Node, reader io.Reader) (error, map[int][]string)

// ReduceInterface 需要客户定制的方法,进行reduce操作，返回结果以及信息
type ReduceInterface func(node *Node, reader io.Reader) (error, []string)

type HandleTaskInterface func(node *Node, task *Task, accept *bool) error

// WorkerInterface 基础节点需要实现的方法
type WorkerInterface interface {
	HandleTask(task *Task, accept *bool) error //处理master发布的任务
	register() error                           //将自身注册至master
	pong() error                               //接受master的ping表示自己的状态
	info(msg interface{}) error                //通知master最终的执行信息
}

// Node 基础节点，提供mapreduce框架功能
type Node struct {
	masterAddress, masterPort string //master的地址
	client                    *rpc.Client
	address, port             string //自身的地址
	nodeType                  string //节点的类型
	mapFunc                   MapperInterface
	reduceFunc                ReduceInterface
	handleTaskFunc            HandleTaskInterface
}

func NewNode(address string, port string, nodeType string) *Node {
	return &Node{address: address, port: port, nodeType: nodeType}
}

func (node *Node) WithMasterConfig(masterAddr, masterPort string) error {
	node.masterAddress = masterAddr
	node.masterPort = masterPort
	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%s", masterAddr, masterPort))
	if err != nil {
		return err
	}
	node.client = client
	return nil
}

func (node *Node) WithMapperFunc(f MapperInterface) error {
	if node.nodeType != MAPPER {
		return fmt.Errorf("Node mismatch ")
	}
	if f == nil {
		return fmt.Errorf("MapperInterface is nil")
	}
	node.mapFunc = f
	return nil
}

func (node *Node) WithReduceFunc(f ReduceInterface) error {
	if node.nodeType != REDUCER {
		return fmt.Errorf("Node mismatch ")
	}
	if f == nil {
		return fmt.Errorf("ReduceInterface is nil")
	}
	node.reduceFunc = f
	return nil
}

func (node *Node) WithHandleTaskFunc(f HandleTaskInterface) error {
	if f == nil {
		return fmt.Errorf("HandleTaskInterface is nil")
	}
	node.handleTaskFunc = f
	return nil
}
