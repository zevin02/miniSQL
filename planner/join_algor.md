# JOIN算法

## 概念
JOIN最常见的算法是Index Nested-Loop Join(NLJ),Block Nested-Loop Join(BLJ),Hash Join算法，以及相关的优化，MBR（Multi-Range Read）,以及BKA（Batched Key Access）

<u>一般来说，驱动表都是表数据比较少的，便于使用索引查询</u>

## Index Nested-Loop Join(NLJ)

~~~sql
# 被驱动表表t2 a列上有索引
select * from t1 straight_join t2 on (t1.a=t2.a);
~~~
对于`被驱动表t2`可以使用`索引查找`

NLJ:
* 从t1来读取一条数据
* 使用索引在t2中进行查找
* 取出匹配的行，和t1中组成一行，作为结果集的一部分
* 重复前面操作，知道t1表读取结束

我们可以发现，在`被驱动表中使用索引`的情况下，使用join效率比手动迭代循环效率低很多
手动JOIN其是和<u>Simple Nested-Loop Join</u>查询的字段没有索引


>假设被驱动表行数为M，需要先走二级索引，在走主键索引，时间复杂度为**O(2*log2M)**
>
>假设驱动表的行数是 N，执行过程就要扫描驱动表 N 行，然后对于每一行，到被驱动表上匹配一次
>
>因此整个执行过程，近似复杂度是 **O(N + N2log2M)**
>
>显然，N 对扫描行数的影响更大，因此应该让`小表`来做驱动表
 
 
## Block Nested-Loop Join(BNL)

Block Nested-Loop Join是对SNL的一个优化
BNL：
* 分块的把t1表的全部数据读取到内存中的join buffer中
* 如果有索引，可以使用索引对t2进行查询，取出符合的数据
* 清空join buffer
* 继续读取t1表，重复上述的操作


>一次取出一个M个块出来，这时再取出一次所有的N块出来进行对比，如次推演

>尽可能多的将外表缓存在内存中，扫描内表的时候，尽量使用索引
 
 
**推荐使用小表来用作驱动表,减少驱动表被扫描的次数，同时减少内存占用**


## Hash Join
Hash Join中只能使用`等值连接`,优先使用`Hash Join`

**等值连接：**使用`=`来连接两个不同的表
~~~sql
SELECT Orders.OrderID, Customers.CustomerName
FROM Orders
INNER JOIN Customers ON Orders.CustomerID = Customers.CustomerID;
~~~

**非等值连接**:涉及到多个操作符，不适用`=`
~~~sql
SELECT *
FROM table1
JOIN table2 ON table1.column_name > table2.column_name;
~~~

<u>Hash Join是对BNL进行的一个优化，只不过是把join buffer中的数据生成一个hash表，从被驱动表中扫描数据得到需要的数据</u>

### Simple Hash Join
* build： 将较小的表需要连接的哪个族作为key，并将记录插入到一张表中
* probe：将较大的表使用和build中同样的hash函数，在hash表中查找是否存在某个值，并取出

不过现在这样会存在了frequency wall,memory wall的瓶颈，CPU从高频单核架构演变成低频多核架构，内存容量的增长，导致存内存数据库的流行，JOIN的开销从IO瓶颈，转化成了CPU瓶颈，因为shj算法在往hash表中插入记录和从hash表中读取记录都有大量的随机内存读取，而随机读取比顺序访问开销大


对于不存在的可以使用bloom filter来进行过滤，减少不必要的查找,这样速度非常块，对两个表中所有值进行遍历



### 高性能hash join的实现
Partitioned Hash Join
分区哈希连接，在执行构建之前会对输入的关系进行分区操作，对每个分区使用Simple Hash Join算法

例如：
>有两个表R和S
> 1. 分区：将R和S按照连接的key进行分区，将R分成R1,R2,R3，将S分成S1,S2,S3，按道理来说：因为是对同一个连接key进行分块，所以相同的key会被分在同一个分区当中，所以可以锁的粒度，以及尽可能多的放在cache中
> 2. 构建：针对每个关系的分区，使用shj算法分别构建哈希表，对于每个分区使用相同的hash函数，并将记录插入到ri的哈希表中，对于s表也同样构建哈希表
> 3. probe：对于每个分区的RI，使用相同的哈希函数计算哈希值，再si的表中查找到匹配的记录

>分区多可以增加hash bucket，减少锁的粒度，减小抢占的冲突风险，同时可以把一个hash bucket放入到cache中，
> 
>把relation进行不断的分区，把一个分区的哈希表可以放入


### 优化，向量化
SIMD(单指令多数据)：计算机指令集的扩展，可以在一个指令中同时处理多个数据，把多个数据大包成一个向量，提高程序的并行度和运算速度

我们将hash冲突进行向量化，其加速效果取决于hash join算法全过程有多少部分能够被完全的向量化，数据尽可能多的存储在向量寄存器中

~~~markdown
您可以使用`github.com/minio/simdjson-go`库中的SIMD向量化技术来加速哈希连接操作。该库提供了高效的JSON解析和处理功能，使用SIMD指令来提高性能。

要使用该库进行哈希连接操作，您可以按照以下步骤进行修改：

1. 首先，确保已安装`github.com/minio/simdjson-go`库。您可以使用以下命令进行安装：

   ```shell
   go get -u github.com/minio/simdjson-go
   ```

2. 在代码中导入所需的包：

   ```go
   import (
   	"github.com/minio/simdjson-go"
   )
   ```

3. 使用`simdjson-go`库中的函数解析JSON数据并构建哈希表。您可以使用`simdjson.Parse([]byte)`函数将JSON数据解析为SIMD JSON对象。

4. 对表R和S进行分区操作，使用SIMD JSON对象构建哈希表。您可以按照以下步骤进行修改：

   - 在`partition`函数中，将输入数据解析为SIMD JSON对象。
   - 在`buildHash`函数中，使用SIMD JSON对象构建哈希表。

5. 在`probe`函数中，使用SIMD JSON对象进行探测操作。您可以根据记录的连接键在SIMD JSON对象中查找匹配的记录。

6. 最后，在`merge`函数中合并不同分区的符合条件的记录。您可以使用SIMD JSON对象进行合并操作。

通过使用`github.com/minio/simdjson-go`库中的SIMD向量化技术，您可以加速哈希连接操作，并提高性能和效率。请参考该库的文档和示例代码以了解更多详细信息和用法。


Hash Join 是通过使用 SIMD 向量化指令来实现加速的。向量化指令属于 SIMD 类型，即单指令多数据。在 Hash Join 中，如果一个向量中的多个 key 值内出现了 hash conflict，理论上就出现了至少两个 branch（无冲突 key 的处理和有冲突 key 的处理），而 SIMD 一定永远在向量上使用相同的指令，这会导致分支预测失败，从而影响性能。

为了解决这个问题，Hash Join 使用 SIMD 的 gather 和 scatter 命令来处理 hash 冲突。具体来说，Hash Join 首先将存储了多个 hash 值的向量按照 scatter 的方式写进内存中，然后通过 gather 读回来获得新的向量。接着，Hash Join 比较原始向量和新的向量的内容，找出因 hash 冲突而被覆盖的位置。最后，Hash Join 利用 mask 将这些位置标记出来，留到下一个迭代再插入 hash 表中，从而减少哈希冲突，提高性能。

~~~




~~~go
package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"strconv"
	"strings"
)

type Record struct {
	Key   string
	Value string
}

func main() {
	// 读取小表S
	s, err := readTable("s.txt")
	if err != nil {
		fmt.Println("读取小表S出错：", err)
		return
	}

	// 读取表B
	b, err := readTable("b.txt")
	if err != nil {
		fmt.Println("读取表B出错：", err)
		return
	}

	// 第一步：判断是否可以进行内存哈希连接
	if canMemoryHashJoin(s) {
		result := memoryHashJoin(s, b)
		fmt.Println(result)
		return
	}

	// 第二步：决定分区数
	numPartitions := decideNumPartitions(s)

	// 第三步：读取小表S，进行哈希映射和创建哈希表
	partitions := make([][]Record, numPartitions)
	hashTables := make([]map[string]string, numPartitions)
	for i := range partitions {
		partitions[i] = []Record{}
		hashTables[i] = make(map[string]string)
	}
	for _, record := range s {
		partitionIndex := hash(record.Key) % numPartitions
		partitions[partitionIndex] = append(partitions[partitionIndex], record)
		hashTables[partitionIndex][record.Key] = record.Value
	}

	// 第四步：建立位图向量
	bitmaps := make([][]bool, numPartitions)
	for i := range bitmaps {
		bitmaps[i] = make([]bool, len(b))
	}
	for i, record := range b {
		partitionIndex := hash(record.Key) % numPartitions
		bitmaps[partitionIndex][i] = true
	}

	// 第五步：如果内存不足，将分区写入磁盘
	for i, partition := range partitions {
		if len(partition)*2 > hashAreaSize() {
			writePartition(i, partition)
			partitions[i] = nil
			hashTables[i] = nil
		}
	}

	// 第六步：读取小表S的剩余部分，重复第三步，直到读取完整个小表S
	for _, record := range s {
		if partitions[hash(record.Key)%numPartitions] != nil {
			continue
		}
		partitionIndex := hash(record.Key) % numPartitions
		partitions[partitionIndex] = append(partitions[partitionIndex], record)
		hashTables[partitionIndex][record.Key] = record.Value
		if len(partitions[partitionIndex])*2 > hashAreaSize() {
			writePartition(partitionIndex, partitions[partitionIndex])
			partitions[partitionIndex] = nil
			hashTables[partitionIndex] = nil
		}
	}

	// 第七步：按大小对分区进行排序，选取多个分区建立哈希表
	selectedPartitions := selectPartitions(partitions)

	// 第八步：根据哈希值建立哈希表
	for _, partitionIndex := range selectedPartitions {
		hashTable := make(map[string]string)
		for _, record := range partitions[partitionIndex] {
			hashTable[record.Key] = record.Value
		}
		hashTables[partitionIndex] = hashTable
	}

	// 第九步：读取表B，使用位图向量进行过滤
	filteredB := make([]Record, 0, len(b))
	for i, record := range b {
		partitionIndex := hash(record.Key) % numPartitions
		if bitmaps[partitionIndex][i] {
			filteredB = append(filteredB, record)
		}
	}

	// 第十步：将过滤后的数据映射到相应的分区，并计算哈希值
	for _, record := range filteredB {
		partitionIndex := hash(record.Key) % numPartitions
		if hashTables[partitionIndex] == nil {
			continue
		}
		value, ok := hashTables[partitionIndex][record.Key]
		if ok {
			result := join(record.Value, value)
			writeResult(result)
			continue
		}
		partitions[partitionIndex] = append(partitions[partitionIndex], record)
		hashTables[partitionIndex][record.Key] = record.Value
		if len(partitions[partitionIndex])*2 > hashAreaSize() {
			writePartition(partitionIndex, partitions[partitionIndex])
			partitions[partitionIndex] = nil
			hashTables[partitionIndex] = nil
			selectedPartitions = selectPartitions(partitions)
			for _, partitionIndex := range selectedPartitions {
				hashTable := make(map[string]string)
				for _, record := range partitions[partitionIndex] {
					hashTable[record.Key] = record.Value
				}
				hashTables[partitionIndex] = hashTable
			}
			bitmaps = make([][]bool, numPartitions)
			for i := range bitmaps {
				bitmaps[i] = make([]bool, len(b))
			}
			for i, record := range b {
				partitionIndex := hash(record.Key) % numPartitions
				bitmaps[partitionIndex][i] = true
			}
			filteredB = make([]Record, 0, len(b))
			for i, record := range b {
				partitionIndex := hash(record.Key) % numPartitions
				if bitmaps[partitionIndex][i] {
					filteredB = append(filteredB, record)
				}
			}
			for _, record := range filteredB {
				partitionIndex := hash(record.Key) % numPartitions
				if hashTables[partitionIndex] == nil {
					continue
				}
				value, ok := hashTables[partitionIndex][record.Key]
				if ok {
					result := join(record.Value, value)
					writeResult(result)
					continue
				}
				partitions[partitionIndex] = append(partitions[partitionIndex], record)
				hashTables[partitionIndex][record.Key] = record.Value
			}
			continue
		}
    }

    // 第十三步：读取(Si,Bi)进行哈希连接，可能发生动态角色互换

    // 第十四步：如果分区后最小的分区仍大于内存，则进行嵌套循环哈希连接

	fmt.Println("算法执行完毕")
}

func readTable(filename string) ([]Record, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	var records []Record

	for {
		line, err := reader.ReadString('\n')
        if err != nil && err != io.EOF {
            return nil, err
        }
        if line == "" && err == io.EOF {
            break
        }

        parts := strings.Split(strings.TrimSpace(line), ",")
        if len(parts) != 2 {
            return nil, fmt.Errorf("invalid format: %s", line)
        }

        records = append(records, Record{Key: parts[0], Value: parts[1]})
    }

    return records, nil
}

func canMemoryHashJoin(s []Record) bool {
    return len(s)*2 <= hashAreaSize()
}

func memoryHashJoin(s []Record, b []Record) []string {
    result := make([]string, 0)

    sMap := make(map[string]string)
    for _, record := range s {
        sMap[record.Key] = record.Value
    }

    for _, record := range b {
        if value, ok := sMap[record.Key]; ok {
            result = append(result, join(record.Value, value))
        }
    }

    return result
}

func decideNumPartitions(s []Record) int {
    clusterSize := dbBlockSize() * hashMultiblockIOCount()
    fAvailMem := 0.8 // 假设可用内存为总内存的80%
    hashAreaSize := hashAreaSize()
    return int(float64(hashAreaSize) / float64(clusterSize*fAvailMem))
}

func hash(key string) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32())
}

func dbBlockSize() int64 {
    return 8192 // 假设为8KB
}

func hashMultiblockIOCount() int64 {
    return 8 // 假设为8块IO操作同时进行
}

func hashAreaSize() int {
    return 1024 * 1024 * 1024 // 假设为1GB
}

func writePartition(partitionIndex int, partition []Record) error {
    filename := fmt.Sprintf("partition_%d.txt", partitionIndex)
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    writer := bufio.NewWriter(file)

    for _, record := range partition {
        line := fmt.Sprintf("%s,%s\n", record.Key, record.Value)
        _, err := writer.WriteString(line)
        if err != nil {
            return err
        }
    }

    return writer.Flush()
}

func selectPartitions(partitions [][]Record) []int {
    type partitionInfo struct {
        index int
        size  int
    }

    infos := make([]partitionInfo, len(partitions))
    for i, partition := range partitions {
        infos[i].index = i
        infos[i].size = len(partition)
    }

    selectedInfos := make([]partitionInfo, 0)
    sizeSum := 0

    for sizeSum < hashAreaSize()/2 && len(infos) > 0 {
        maxInfoIndex := 0

        for i := 1; i < len(infos); i++ {
            if infos[i].size > infos[maxInfoIndex].size {
                maxInfoIndex = i
            }
        }

        selectedInfos = append(selectedInfos, infos[maxInfoIndex])
        sizeSum += infos[maxInfoIndex].size

        infos[maxInfoIndex], infos[len(infos)-1] = infos[len(infos)-1], infos[maxInfoIndex]
        infos = infos[:len(infos)-1]
    }

    result := make([]int, len(selectedInfos))
    for i, info := range selectedInfos {
        result[i] = info.index
    }

    return result
}

func join(value1 string, value2 string) string {
    return value1 + "," + value2 + "\n"
}

func writeResult(result string) error {
    file, err := os.OpenFile("result.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer file.Close()

    writer := bufio.NewWriter(file)

    _, err = writer.WriteString(result)
    if err != nil {
        return err
    }

    return writer.Flush()
}
~~~