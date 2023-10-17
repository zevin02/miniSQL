package planner

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
func buildHash(records []Record) map[int]string {
	hashTable := make(map[int]string)
	for _, record := range records {
		hashTable[record.Key] = record.Data
	}
	return hashTable
}

// 探测操作,在r分区中，使用s分区已经构造好了的哈希表进行探测
func probe(partition []Record, hashTable map[int]string) []Record {
	var result []Record
	//遍历r分区的数据，r分区就是驱动表，记录一般比较小，可以进行分块读取
	for _, record := range partition {
		//使用s分区的哈希表进行探测
		if data, ok := hashTable[record.Key]; ok {
			result = append(result, Record{Key: record.Key, Data: data})
		}
	}
	//获得当前分区中符合条件的记录
	return result
}

// 合并结果,将不同区块的符合条件的数据进行合并
func merge(records1, records2 []Record) []Record {
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
