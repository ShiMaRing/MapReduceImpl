package main

// WorkerInterface 基础节点需要实现的方法
type WorkerInterface interface {
	handleTask(task *Task, reply *string) error //处理master发布的任务
	register() error                            //将自身注册至master
	pong() error                                //接受master的ping表示自己的状态
	info(msg interface{}) error                 //通知master最终的执行信息
}
