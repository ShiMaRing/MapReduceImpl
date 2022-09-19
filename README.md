# MapReduceImpl


使用 `Golang` 实现的简单 `MapReduce` 框架 ，使用`rpc`进行通信 

框架进行了简单的封装，具有一定的容错能力，目前尚未实现 `心跳机制` 

目前暂未支持`Map` 与 `Reduce` 节点远程调取文件，预留有实现接口

用户只需要配置 ` M  R `, 各节点的 ip 与 端口 并实现  `Map` 以及 `Reduce` 方法即可进行使用

### 代码结构

```
master.go    //master节点的主要实现

worker.go    //worker节点的主要实现

model.go     //公用依赖

source       //样例数据     
```

### 使用案例（字符数量统计的代码实现）

```  go
import (
	"log"
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
				log.Println(err)
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




``` 
