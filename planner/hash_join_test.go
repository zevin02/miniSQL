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

	//驱动表在内存中的buffer中构建成一个哈希表和布隆过滤器,被驱动表不需要得到其的哈希表和布隆过滤器，直接进行将两者进行比对即可
	// 分区操作
	numPartitions := 2                         //分了两个分区
	rPartitions := partition(r, numPartitions) //r分区的结果
	sPartitions := partition(s, numPartitions) //s分区的结果

	// 构建哈希表
	var rHashTables []map[uint64]Record //每个分区都构造一个哈希表
	var rBloomFilter []*bloom.BloomFilter

	for i := 0; i < numPartitions; i++ { //给每个分区分别创建哈希表和布隆过滤器
		rht, rbf := buildHash(rPartitions[i])
		rHashTables = append(rHashTables, rht)   //给各个分区分别构建哈希表
		rBloomFilter = append(rBloomFilter, rbf) //给各个分区分别构建哈希表
	}

	ret := HashJoin(sPartitions, rHashTables, rBloomFilter)

	fmt.Println(ret)

}
