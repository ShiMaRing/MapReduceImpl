package main

type state string

const (
	FINISHED state  = "finish"
	FAILED   state  = "failed"
	MAPPER   string = "mapper"
	REDUCER  string = "reducer"
)

// Task 等待master分配任务
type Task struct {
	taskType  string              //任务类型
	fileName  string              //分配给mapper的文件名
	R         int                 //分配给mapper的R的大小
	filePaths map[string][]string //key 为worker地址，v为文件名列表
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

// MapResult  最终执行的结果
type MapResult struct {
	mapperId int
	state    state
	res      map[int][]string
}

// ReduceResult   最终执行的结果
type ReduceResult struct {
	reducerId int
	state     state
	errType   string
	res       []string //最终的结束文件
}
