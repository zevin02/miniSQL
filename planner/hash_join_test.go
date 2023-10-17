package planner

import "testing"

func TestHashJoin(t *testing.T) {
	// 创建关系R和S
	r := []Record{
		{Key: 1, Data: "A"},
		{Key: 2, Data: "B"},
		{Key: 3, Data: "C"},
		{Key: 4, Data: "D"},
		{Key: 5, Data: "E"},
	}
	s := []Record{
		{Key: 1, Data: "a"},
		{Key: 2, Data: "b"},
		{Key: 3, Data: "c"},
		{Key: 4, Data: "d"},
		{Key: 5, Data: "e"},
	}
	// 分区操作
	numPartitions := 2
	rPartitions := partition(r, numPartitions) //r分区的结果
	sPartitions := partition(s, numPartitions) //s分区的结果

	// 构建哈希表
	var rHashTables []map[int]string //每个分区都构造一个哈希表
	var sHashTables []map[int]string
	for i := 0; i < numPartitions; i++ {
		rHashTables = append(rHashTables, buildHash(rPartitions[i])) //给各个分区分别构建哈希表
		sHashTables = append(sHashTables, buildHash(sPartitions[i]))
	}

	// 探测操作
	var results [][]Record //各个分区中符合条件的记录
	for i := 0; i < numPartitions; i++ {
		results = append(results, probe(rPartitions[i], sHashTables[i]))
	}

	// 合并结果
	finalResult := results[0]
	for i := 1; i < numPartitions; i++ {
		finalResult = merge(finalResult, results[i])
	}
}
