package main

import "sync"

type Status int

const (
	IDLE Status = iota
	RUNNING
	FINISH
	DEAD
)

type WorkerID int
type ReduceID int

// Master 节点
type Master struct {
	M, R       uint32            //M和R的值
	cnt        uint32            //当前已经完成的reduce任务数量
	mappers    nodes             //当前注册的所有mapper
	reducers   nodes             //当前注册的所有reducers
	workersMap map[WorkerID]node //标记是否存在相应worker
	mapperTask []string          //当前需要处理的mapperTasks,由M决定,存放需要处理的文件名称
	reduceTask []int             //当前需要处理的reduceTasks 由R决定 ，存放reduceId
	//当前正在执行的任务
	tasksMap map[WorkerID]*Task
	//当前已经完成map任务
	mapResultMap map[WorkerID]map[ReduceID][]string
	reduceResult map[WorkerID][]string

	mu sync.Mutex //互斥锁
}

func (master *Master) Register(workInfo *WorkerInfo, res *string) error {

	return nil
}

type node struct {
	info   *WorkerInfo
	status Status
}

type nodes []node

func (n nodes) Len() int {
	return len(n)
}

// Less 按照score大小排序
func (n nodes) Less(i, j int) bool {
	return n[i].info.score >= n[j].info.score
}

func (n nodes) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}
