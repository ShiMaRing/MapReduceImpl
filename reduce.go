package main

import "io"

// ReduceResult   最终执行的结果
type ReduceResult struct {
	taskId uint64
	state  string
	res    []string //最终的结束文件
}

// ReduceInterface 需要客户定制的接口,进行reduce操作，返回结果以及信息
type ReduceInterface interface {
	reduce(reader io.Reader) (error, []string)
}
