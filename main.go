package main

import (
	"log"
	"strconv"
	"xgs/mp"
)

func main() {
	const M, R = 3, 2
	master := mp.NewMaster(M, R, "", "8080") //启动master远程服务
	go func() {
		err := master.Start()
		if err != nil {
			panic(err)
		}
	}()
	_ = master.AddTasks([]string{"source/1.dat", "source/2.dat", "source/3.dat"}) //添加任务

	//创建map节点
	for i := 0; i < 5; i++ {
		port := 8090 + i
		newNode := mp.NewNode("", strconv.Itoa(port), mp.MAPPER, i)
		newNode.WithMasterConfig("", "8080")
		newNode.WithMapperFunc(TestMap) //设置map函数
		err := newNode.Register()       //注册到master

		go func() {
			err = newNode.Start()
			if err != nil {
				log.Println(err)
			}
		}()
	}

	//创建reduce节点
	for i := 5; i < 8; i++ {
		port := 8090 + i
		newNode := mp.NewNode("", strconv.Itoa(port), mp.REDUCER, i)
		newNode.WithMasterConfig("", "8080")
		newNode.WithReduceFunc(TestReduce) //设置reduce函数
		err := newNode.Register()          //注册到master
		if err != nil {
			log.Println(err)
		}
		go func() {
			err = newNode.Start()
			if err != nil {
				log.Println(err)
			}
		}()
	}
	//启动调度master并接收结果
	result := master.StartSchedule()
	log.Println("the output file is : ", result)
}
