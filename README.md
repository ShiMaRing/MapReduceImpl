# MapReduceImpl

## 云计算架构与技术课堂作业


使用 `Golang` 实现的简单 `MapReduce` 框架 ，使用`rpc`进行通信 

框架进行了简单的封装，具有一定的容错能力，目前尚未实现 `心跳机制` 以及 `reducer` 与`mapper`之间的远程通信 

用户只需要配置 ` M  R `, 各节点的 ip 与 端口 并实现  `Map` 以及 `Reduce` 方法即可进行使用



