package planner

import (
	"encoding/binary"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/twmb/murmur3"
	"hash"
	"sort"
	"sync"
)

//假设有两个表R，S，需要根据连接键做连接操作
type Record struct {
	Key  int
	Data string
}

var h hash.Hash64

// 分区操作,传入需要分区的数据，以及需要分多少个区,传出的就是各个分区的结果
func partition(records []Record, numPartitions int) [][]Record {
	partitions := make([][]Record, numPartitions)
	for _, record := range records {

		partitionIndex := record.Key % numPartitions //把当前的分区进行取余，获得其被划分在哪个分区中
		partitions[partitionIndex] = append(partitions[partitionIndex], record)
	}
	//先进行数据的分区
	return partitions
}

func getHashValue(key int) uint64 {

	newData := encoded(uint32(key)) //先把当前的key转化成字符数组的形式
	h.Write(newData)
	newHashValue := h.Sum64()

	return newHashValue
}

// 构建哈希表,为各个分区分别构造哈希表，以及bloom filter表，加快不存在数据的排查
//map中的key就是当前的记录的连接key，value就是当前对应的记录,
func buildHash(records []Record) (map[uint64]Record, *bloom.BloomFilter) {
	h = murmur3.New64()
	hashTable := make(map[uint64]Record) //key是这个哈希值，value就是他的这条记录
	filter := bloom.NewWithEstimates(1000000, 0.01)
	for _, record := range records {
		hashValue := getHashValue(record.Key)
		hashTable[hashValue] = record
		filter.Add(encoded(uint32(hashValue))) //同样是把当前的这个哈希值进行编码放到布隆过滤器中进行处理
	}
	return hashTable, filter
}

func encoded(key uint32) []byte {
	n1 := make([]byte, 4)
	binary.BigEndian.PutUint32(n1, key)
	return n1
}

// 探测操作,在r分区中，使用s分区已经构造好了的哈希表进行探测,把被驱动表的布隆过滤器提取上来进行检测
//从被驱动表中的一条一条的记录，读取,往管道中发送数据
//go中的map不支持并发的读取和写入
func probe(drivenTable []Record, driverHashTable map[uint64]Record, driverBloomFilter *bloom.BloomFilter, wg *sync.WaitGroup, resultCh chan<- []Record) {
	defer wg.Done()
	var result []Record
	//遍历r分区的数据，r分区就是驱动表，记录一般比较小，可以进行分块读取
	for _, record := range drivenTable {
		hashValue := getHashValue(record.Key)
		//使用s分区的哈希表进行探测，遍历当前分区驱动表的数据，先在被驱动表的布隆过滤器中检查数据是否存在
		if driverBloomFilter.Test(encoded(uint32(hashValue))) {
			//当前数据如果存在了，才能在哈希表中检查，获取到相应的数据
			//如果当前在bloom filter中查找到就验证，如果当前没有找到，直接就可以返回了
			if data, ok := driverHashTable[hashValue]; ok {
				//检查发现key相同，所以当前的数据可以被提取出来,把驱动表中需要的数据提取出来,并返回当前的组合数据
				//TODO 之后时使用schema加入当前schema表的结构
				result = append(result, Record{Key: record.Key, Data: data.Data + record.Data})
			}
		}

	}
	//获得当前分区中符合条件的记录
	if len(result) > 0 {
		resultCh <- result // 把当前筛选出来的数据通过管道发送
	}
}

//HashJoin 使用goruntine来实现数据的并发读取比对，并返回hash join的结果
func HashJoin(drivenPartions [][]Record, driverHashTable []map[uint64]Record, driverBloomFilter []*bloom.BloomFilter) []Record {
	var result []Record //汇总所有符合条件的结果
	var wg sync.WaitGroup
	h = murmur3.New64()
	resultCh := make(chan []Record, len(drivenPartions)) //多个线程同时向一个管道中发送数据
	for partionIndex, partion := range drivenPartions {
		wg.Add(1)
		//使用go routine来并发的读取数据，因为他们互不影响
		go probe(partion, driverHashTable[partionIndex], driverBloomFilter[partionIndex], &wg, resultCh)
	}
	go func() {
		//启动一个单独的goruntine来等待所有的probe函数完成
		wg.Wait()
		close(resultCh)
	}()
	for r := range resultCh {
		//遍历所有的东西
		result = append(result, r...)
	}
	//按照连接的key进行升序排列
	sortByKey := func(records []Record) {
		sort.Slice(records, func(i, j int) bool {
			return records[i].Key < records[j].Key
		})
	}

	//这里把result按照连接的key来进行排序
	sortByKey(result)
	return result

}
