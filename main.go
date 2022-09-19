package main

import (
	"fmt"
	"strconv"
)

func main() {
	const M, R = 3, 2
	master := NewMaster(M, R, "", "8080") //启动master远程服务
	go func() {
		err := master.Start()
		if err != nil {
			panic(err)
		}
	}()
	_ = master.AddTasks([]string{"source/1.dat", "source/2.dat", "source/3.dat"}) //添加任务

	//创建map节点
	for i := 0; i < 2; i++ {
		port := 8090 + i
		newNode := NewNode("", strconv.Itoa(port), MAPPER, i)
		newNode.WithMasterConfig("", "8080")
		newNode.WithMapperFunc(TestMap) //设置map函数
		err := newNode.Register()       //注册到master

		go func() {
			err = newNode.Start()
			if err != nil {
				fmt.Println(err)
			}
		}()
	}

	//创建reduce节点
	for i := 3; i < 5; i++ {
		port := 8090 + i
		newNode := NewNode("", strconv.Itoa(port), REDUCER, i)
		newNode.WithMasterConfig("", "8080")
		newNode.WithReduceFunc(TestReduce) //设置reduce函数
		err := newNode.Register()          //注册到master
		if err != nil {
			fmt.Println(err)
		}
		go func() {
			err = newNode.Start()
			if err != nil {
				fmt.Println(err)
			}
		}()
	}

	//启动调度master并接收结果
	result := master.StartSchedule()
	fmt.Println(result)
}
