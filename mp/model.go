package mp

type state string
type NodeType string

const (
	FINISHED state    = "finish"
	FAILED   state    = "failed"
	MAPPER   NodeType = "mapper"
	REDUCER  NodeType = "reducer"
)

// Task 等待master分配任务
type Task struct {
	TaskType  NodeType            //任务类型
	FileName  string              //分配给mapper的文件名
	R         int                 //分配给mapper的R的大小
	FilePaths map[string][]string //key 为worker地址，v为文件名列表
	Cur       int                 //当前reduce要合并的reduce任务
}

// WorkerInfo  将自己注册到master中携带的信息
// 为自己的地址以及端口，自己的节点类型，还有自己的score评分表示自己的性能，会根据性能优先调度
type WorkerInfo struct {
	Id         int //workerId,标记worker
	Address    string
	Port       string
	WorkerType NodeType
	Score      uint32
}

// MapResult  最终执行的结果
type MapResult struct {
	MapperId int
	State    state
	ErrType  string
	Res      map[int][]string
}

// ReduceResult   最终执行的结果
type ReduceResult struct {
	ReducerId int
	State     state
	ErrType   string
	Res       []string //最终的结束文件
}
