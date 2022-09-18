package main

// Task 等待master分配任务
type Task struct {
	tasKId   uint64 //任务id
	taskType string //任务类型
	inner    data   //内部数据
}

type data struct {
	fileName  string            //分配给mapper的文件名
	filePaths map[string]string //key 为worker地址，v为文件名
	reduceId  int               //reduce的任务id，找到相关的文件进行处理
	mapperId  int               //mapper的任务id，找到相关的文件进行处理
}

// Pair 键值对
type Pair struct {
	k string
	v interface{}
}

// WorkerInfo  将自己注册到master中携带的信息
// 为自己的地址以及端口，自己的节点类型，还有自己的score评分表示自己的性能，会根据性能优先调度
type WorkerInfo struct {
	id         int //workerId,标记worker
	address    string
	port       string
	workerType string
	score      uint32
}
