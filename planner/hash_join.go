package planner

import (
	"encoding/binary"
	"github.com/bits-and-blooms/bloom/v3"
)

//假设有两个表R，S，需要根据连接键做连接操作
type Record struct {
	Key  int
	Data string
}

// 分区操作,传入需要分区的数据，以及需要分多少个区,传出的就是各个分区的结果
func partition(records []Record, numPartitions int) [][]Record {
	partitions := make([][]Record, numPartitions)
	for _, record := range records {
		partitionIndex := record.Key % numPartitions //把当前的分区进行取余，获得其被划分在哪个分区中
		partitions[partitionIndex] = append(partitions[partitionIndex], record)
	}
	return partitions
}

// 构建哈希表,为各个分区分别构造哈希表，以及bloom filter表，加快不存在数据的排查
//map中的key就是当前的记录的连接key，value就是当前对应的记录,
func buildHash(records []Record) (map[int]string, *bloom.BloomFilter) {
	hashTable := make(map[int]string)
	filter := bloom.NewWithEstimates(1000000, 0.01)
	for _, record := range records {
		hashTable[record.Key] = record.Data
		filter.Add(encoded(uint32(record.Key)))
	}
	return hashTable, filter
}

func encoded(key uint32) []byte {
	n1 := make([]byte, 4)
	binary.BigEndian.PutUint32(n1, key)
	return n1
}

// 探测操作,在r分区中，使用s分区已经构造好了的哈希表进行探测,把被驱动表的布隆过滤器提取上来进行检测
func probe(partition []Record, hashTable map[int]string, bloomFilter *bloom.BloomFilter) []Record {
	var result []Record
	//遍历r分区的数据，r分区就是驱动表，记录一般比较小，可以进行分块读取
	for _, record := range partition {
		//使用s分区的哈希表进行探测，遍历当前分区驱动表的数据，先在被驱动表的布隆过滤器中检查数据是否存在，加速排非
		if bloomFilter.Test(encoded(uint32(record.Key))) {
			//当前数据如果存在了，才能在哈希表中检查，获取到相应的数据
			//如果当前在bloom filter中查找到就验证，如果当前没有找到，直接就可以返回了
			if data, ok := hashTable[record.Key]; ok {
				//检查发现key相同，所以当前的数据可以被提取出来,把驱动表中需要的数据提取出来,并返回当前的组合数据
				result = append(result, Record{Key: record.Key, Data: data + record.Data})
			}
		}

	}
	//获得当前分区中符合条件的记录
	return result
}

// 合并结果,将不同区块的符合条件的数据进行合并
func merge(records1, records2 []Record) []Record {
	//不同的表中数据不同
	var result []Record
	for _, record1 := range records1 {
		for _, record2 := range records2 {
			if record1.Key == record2.Key {
				result = append(result, Record{Key: record1.Key, Data: record1.Data + record2.Data})
			}
		}
	}
	return result
}
