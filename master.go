package main

import (
	"fmt"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

type Status int

const (
	IDLE Status = iota
	RUNNING
	DEAD

	MAXFAIL int = 3
)

type WorkerID int
type ReduceID int

// Master 节点
type Master struct {
	Address, Port           string
	M, R                    int                                //M和R的值
	mapFinish, reduceFinish bool                               //标记是否完成全部任务
	mapCnt, reduceCnt       int                                //记录当前的任务完成数量
	mappers                 nodes                              //当前空闲的所有mapper
	reducers                nodes                              //当前空闲的所有reducers
	workers                 map[WorkerID]*node                 //记录当前注册的所有worker
	mapperTaskMap           map[string]struct{}                //当前需要处理的mapperTasks,去重
	reduceTaskMap           map[int]struct{}                   //当前需要处理的reduceTasks 去重
	mapperTaskChan          chan string                        //当前需要处理的mapperTasks,由M决定,存放需要处理的文件名称 分配
	reduceTaskChan          chan int                           //当前需要处理的reduceTasks 由R决定 ，存放reduceId 分配
	tasksMap                map[WorkerID]*Task                 //当前worker与任务的映射
	mapResultMap            map[WorkerID]map[ReduceID][]string //当前已经完成map任务
	reduceResult            map[WorkerID][]string              //当前已经完成reduce任务
	mu                      sync.Mutex                         //互斥锁,保护workers信息
	resultLock              sync.Mutex                         //互斥锁，保护result信息
}

//是否结束
func (master *Master) isFinished() bool {
	master.mu.Lock()
	defer master.mu.Unlock()
	return master.reduceFinish
}

// NewMaster master的构造函数
func NewMaster(m int, r int, address string, port string) *Master {
	master := &Master{M: m, R: r, Address: address, Port: port}
	master.mappers = make(nodes, 0)
	master.reducers = make(nodes, 0)
	master.workers = make(map[WorkerID]*node)
	master.mapperTaskMap = make(map[string]struct{})
	master.reduceTaskMap = make(map[int]struct{})
	master.tasksMap = make(map[WorkerID]*Task)
	master.mapResultMap = make(map[WorkerID]map[ReduceID][]string) //当前已经完成map任务
	master.reduceResult = make(map[WorkerID][]string)
	master.mapperTaskChan = make(chan string, m)
	master.reduceTaskChan = make(chan int, r)
	return master
}

// StartSchedule 十秒钟调度一次
func (master *Master) StartSchedule() {
	go func() {
		tick := time.Tick(10 * time.Second)
		for !master.isFinished() {
			select {
			case <-tick:
				master.schedule()
			}
		}

		//结束了提交所有的信息
		master.Finish()
	}()
}

// Finish 结束调用，通知所有client结束
func (master *Master) Finish() {

}

// MapperSubmit map提交结果
func (master *Master) MapperSubmit(result *MapResult, reply *bool) error {
	if result == nil {
		*reply = false
		return fmt.Errorf("result is empty")
	}
	id := WorkerID(result.mapperId)

	master.mu.Lock()
	if _, ok := master.tasksMap[id]; !ok {
		*reply = false
		return fmt.Errorf("the worker is not register")
	}
	delete(master.tasksMap, id) //删除关联
	n := master.workers[id]
	n.status = IDLE
	if result.state == FAILED {
		n.failCnt++
		*reply = false
		return nil
	} else {
		master.mapCnt++
		//map任务结束，但是还不能够关闭channel，因为一旦节点失效，数据将无法获取，在reduce结束才可以关闭chanel
		if master.mapCnt == master.M {
			master.mapFinish = true
			close(master.mapperTaskChan)
		}
	}
	master.mappers = append(master.mappers, n)
	master.mu.Unlock()
	master.resultLock.Lock()
	defer master.resultLock.Unlock()
	//添加到结果中去
	for reduceID := range master.mapResultMap[id] {
		master.mapResultMap[id][reduceID] = append(master.mapResultMap[id][reduceID], result.res[int(reduceID)]...)
	}

	*reply = true
	return nil
}

// ReduceSubmit  reduce提交结果
func (master *Master) ReduceSubmit(result *ReduceResult, reply *bool) error {
	if result == nil {
		*reply = false
		return fmt.Errorf("result is empty")
	}
	id := WorkerID(result.reducerId)

	master.mu.Lock()
	if _, ok := master.tasksMap[id]; !ok {
		*reply = false
		return fmt.Errorf("the worker is not register")
	}
	delete(master.tasksMap, id) //删除关联
	n := master.workers[id]
	n.status = IDLE
	if result.state == FAILED {
		n.failCnt++
		*reply = false
		return nil
	} else {
		master.reduceCnt++
		if master.reduceCnt == master.R {
			master.reduceFinish = true
			close(master.reduceTaskChan)
		}
	}
	master.mappers = append(master.reducers, n)
	master.mu.Unlock()

	master.resultLock.Lock()
	defer master.resultLock.Unlock()
	//添加结果
	master.reduceResult[id] = append(master.reduceResult[id], result.res...)
	*reply = true
	return nil
}

func (master *Master) AddTasks(mapperTasks []string, reduceTasks []int) error {
	master.mu.Lock()
	defer master.mu.Unlock()
	if mapperTasks == nil || len(mapperTasks) == 0 {
		return fmt.Errorf("mapperTasks is empty")
	}
	if reduceTasks == nil || len(reduceTasks) == 0 {
		return fmt.Errorf("reduceTasks is empty")
	}
	for _, v := range mapperTasks {
		if _, ok := master.mapperTaskMap[v]; ok {
			continue
		}
		master.mapperTaskMap[v] = struct{}{}
		master.mapperTaskChan <- v
	}
	for _, v := range reduceTasks {
		if _, ok := master.reduceTaskMap[v]; ok {
			continue
		}
		master.reduceTaskMap[v] = struct{}{}
		master.reduceTaskChan <- v
	}
	return nil
}

// Register RPC远程调用,work将自身注册至master
func (master *Master) Register(workInfo *WorkerInfo, success *bool) error {
	master.mu.Lock()
	defer master.mu.Unlock()
	if workInfo == nil {
		*success = false
		return fmt.Errorf("work info can not be nil")
	}
	if _, ok := master.workers[WorkerID(workInfo.id)]; ok {
		*success = false
		return fmt.Errorf("work already exist")
	}
	n := &node{
		info:   workInfo,
		status: IDLE,
	}
	master.workers[WorkerID(workInfo.id)] = n

	if workInfo.workerType == MAPPER {
		master.mappers = append(master.mappers, n)
		sort.Sort(master.mappers)
	} else if workInfo.workerType == REDUCER {
		master.reducers = append(master.reducers, n)
		sort.Sort(master.reducers)
	}

	*success = true
	return nil
}

// Schedule master进行调度，分配任务
func (master *Master) schedule() {
	master.mu.Lock()
	mapFinished := master.mapFinish
	reduceFinished := master.reduceFinish
	master.mu.Unlock()

	//调度mapper
	if !mapFinished {
		for k := range master.mapperTaskChan {
			if len(master.mappers) == 0 {
				//没有可以调度的mapper了
				break
			}
			n := master.mappers[len(master.mappers)-1]
			master.mappers = master.mappers[:len(master.mappers)-1]
			//不再参与调度
			if n.status == DEAD {
				continue
			}
			go master.mapSchedule(k, n)
		}
	}

	//调度reducer
	if !reduceFinished {
		for k := range master.reduceTaskChan {

			if len(master.reducers) == 0 {
				//没有可以调度的mapper了
				break
			}
			n := master.reducers[len(master.reducers)-1]
			master.reducers = master.reducers[:len(master.reducers)-1]
			//不再参与调度
			if n.status == DEAD {
				continue
			}
			go master.reduceSchedule(k, n)
		}
	}

}

func (master *Master) mapSchedule(k string, n *node) {

	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%s", n.info.address, n.info.port))
	if err != nil {
		master.mapperTaskChan <- k
		return
	}
	task := &Task{
		taskType: "map",
		fileName: k,
		R:        master.R,
	}
	var isSuccess bool
	err = client.Call("Mapper.HandleTask", task, &isSuccess)
	master.mu.Lock()
	defer master.mu.Unlock()
	if err == nil && isSuccess {
		n.failCnt = 0
		n.status = RUNNING
		master.tasksMap[WorkerID(n.info.id)] = task
	} else {
		//重新放回去,并记录
		n.failCnt++
		if n.failCnt >= MAXFAIL {
			n.status = DEAD
		}
		master.mappers = append(master.mappers, n)
	}
}

//分配k id
func (master *Master) reduceSchedule(k int, n *node) {

	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%s", n.info.address, n.info.port))
	if err != nil {
		master.reduceTaskChan <- k
		return
	}
	//此处需要拿到所有的workerId以及其对应的文件名
	task := &Task{
		taskType:  "reduce",
		filePaths: map[string][]string{},
		R:         master.R,
	}
	master.mu.Lock()
	for id := range master.mapResultMap {
		mapper := master.mappers[id]
		address := fmt.Sprintf("%s:%s", mapper.info.address, mapper.info.port)
		task.filePaths[address] = master.mapResultMap[id][ReduceID(k)]
	}
	master.mu.Unlock()
	var isSuccess bool
	err = client.Call("Reducer.HandleTask", task, &isSuccess)

	master.mu.Lock()
	defer master.mu.Unlock()
	if err == nil && isSuccess {
		n.failCnt = 0
		master.tasksMap[WorkerID(n.info.id)] = task
	} else {
		//重新放回去,并记录
		n.failCnt++
		if n.failCnt >= MAXFAIL {
			n.status = DEAD
		}
		master.reducers = append(master.reducers, n)
	}

}

type node struct {
	info    *WorkerInfo
	status  Status
	failCnt int //记录失败次数
}

type nodes []*node

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
