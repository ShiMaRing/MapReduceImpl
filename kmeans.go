package main

import "xgs/mp"

func KnnMap(node *mp.Worker, task *mp.Task) (map[int][]string, error) {

	return nil, nil
}

func KnnReduce(node *mp.Worker, task *mp.Task) ([]string, error) {

	return nil, nil
}
/*
由于时间问题，不提供具体代码的实现，下面是算法改写思路
首先需要将输入文件进行切割，切割为M等份，对于每一份切割后的小文件
使用Map函数，调用传统Knn算法，将其划分为最多R份，需要定义损失函数
当损失函数低于某一阈值时，停止寻找新的质点。

最终将会形成 K份划分后的文件（K<=R），根据最终生成的键也就是Key的坐标
范围划分到不同的R中，之后Reducer将会收集各项文件，由于按照Key的坐标范围进行划分
因此同一Reducer收集到的文件中点位都会落在一个区间范围内，最后收集数据即可。


*/
