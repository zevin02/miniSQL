package planner

import (
	"fmt"
	"github.com/bits-and-blooms/bloom/v3"
	"testing"
)

func TestHashJoin(t *testing.T) {
	// 创建关系R和S
	r := []Record{
		{Key: 1, Data: "A"},
		{Key: 2, Data: "B"},
		{Key: 3, Data: "C"},
		{Key: 4, Data: "D"},
		{Key: 5, Data: "E"},
		{Key: 6, Data: "E"},
	}
	s := []Record{
		{Key: 1, Data: "a"},
		{Key: 2, Data: "b"},
		{Key: 3, Data: "c"},
		{Key: 4, Data: "d"},
		{Key: 5, Data: "e"},
	}
	// 分区操作
	numPartitions := 2                         //分了两个分区
	rPartitions := partition(r, numPartitions) //r分区的结果
	sPartitions := partition(s, numPartitions) //s分区的结果

	// 构建哈希表
	var rHashTables []map[int]string //每个分区都构造一个哈希表
	var sHashTables []map[int]string
	var rBloomFilter []*bloom.BloomFilter
	var sBloomFilter []*bloom.BloomFilter
	for i := 0; i < numPartitions; i++ { //给每个分区分别创建哈希表和布隆过滤器
		rht, rbf := buildHash(rPartitions[i])
		rHashTables = append(rHashTables, rht)   //给各个分区分别构建哈希表
		rBloomFilter = append(rBloomFilter, rbf) //给各个分区分别构建哈希表

		sht, sbf := buildHash(sPartitions[i])
		sHashTables = append(sHashTables, sht)   //给各个分区分别构建哈希表
		sBloomFilter = append(sBloomFilter, sbf) //给各个分区分别构建哈希表

	}

	// 探测操作
	var results [][]Record //各个分区中符合条件的记录
	for i := 0; i < numPartitions; i++ {
		results = append(results, probe(rPartitions[i], sHashTables[i], sBloomFilter[i]))
	}
	//result里面就是各个区块中符合要求的记录
	// 合并结果
	var finalResult []Record
	for _, re := range results {
		finalResult = append(finalResult, re...)
	}
	fmt.Println("he")
}
