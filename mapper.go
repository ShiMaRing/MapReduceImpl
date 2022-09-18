package main

import "io"

// MapResult  最终执行的结果
type MapResult struct {
	taskId uint64
	state  string
	res    map[int][]string
}

// MapperInterface 需要客户定制的接口,读入信息之后进行映射
type MapperInterface interface {
	mapper(reader io.Reader) (error, map[int][]string)
}
