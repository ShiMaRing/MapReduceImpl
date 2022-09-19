package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"
)

func TestMap(node *Worker, task *Task) (res map[int][]string, err error) {
	name := task.FileName //获取目标文件名
	m := make(map[string]int)
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		m[line]++
	}

	res = make(map[int][]string)
	for i := 0; i < task.R; i++ {
		fileName := getName("tmp", "mapper", node.id, i)
		openFile, _ := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0777)
		writer := bufio.NewWriter(openFile)
		for s := range m {
			if hash(s, task.R) == i {
				writer.WriteString(fmt.Sprintf("%s %d\n", s, m[s]))
			}
		}
		writer.Flush()
		openFile.Close()
		res[i] = []string{fileName}
	}
	return
}

func TestReduce(node *Worker, task *Task) ([]string, error) {

	name := getName("output", "reduce", node.id, task.Cur)

	openFile, _ := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0777)
	writer := bufio.NewWriter(openFile)

	m := make(map[string]int)
	//k是地址，v是文件名
	for _, v := range task.FilePaths {
		file, err := os.Open(v[0])
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			split := strings.Split(line, " ")
			count, _ := strconv.Atoi(split[1])
			m[split[0]] += count
		}
	}
	for k, v := range m {
		writer.WriteString(fmt.Sprintf("%s %d\n", k, v))
	}
	writer.Flush()
	return []string{name}, nil
}

func getName(path, prefix string, id int, suffix int) string {
	return fmt.Sprintf("%s/%s-%d-%d", path, prefix, id, suffix)
}

func hash(s string, R int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32()) % R
}
