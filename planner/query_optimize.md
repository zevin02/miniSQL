# 概念

- B(s)：表示执行过程中访问了多少个区块。
- R(s)：表示执行后返回了多少条记录。
- V(s, F)：表示返回记录中字段包含不同值的数量。
- s 对应的是 Scan 对象。

例如，有一张表的格式为 `student name age`，返回了10条记录：
- 3条记录：age=18
- 4条记录：age=19
- 3条记录：age=20

根据上述数据，递归调用这些 Scan 对象的值。

# 查询树

1. project -> select -> customer

   这是一种查询树，先对数据表 customer 进行 select 操作，将所有符合条件的行记录抽取出来，然后进行 project 操作，抽取所需的列。

2. select -> project -> customer

   这也是一种查询树，先对 customer 进行 project 操作，将需要的列筛选出来，然后根据条件筛选出符合条件的行记录。

不同的查询树会对数据操作的效率产生重大影响，因此我们需要构造出所有可能的查询树，并计算不同查询树的执行效率，选择最优的查询树。这个步骤称为 "planning"。

# 计算查询树的效率
在数据库系统中，最消耗资源的操作就是读取硬盘，比读取内存要慢2到3个层级，读硬盘比读内存慢100倍以上
在我们判断查询树执行效率的时候，就需要判断他返回给定数据或记录需要访问几次硬盘，次数越少，效率越高

针对查询树1
~~~go
projectScan := query.NewProductionScan(selectScan, queryData.Fields())
~~~
对于这个B(S),R(S),V(S,F)中的s就对应的projectScan这个变量
我们知道projectscan的next调用selectScan的next接口，
计算B(projectScan)依赖B(selectScan)
![](https://img1.imgtp.com/2023/09/19/eLXcrR1k.jpeg)

![](https://img1.imgtp.com/2023/09/19/NJ7fejcD.jpeg)

![](https://img1.imgtp.com/2023/09/19/ygFVTUHX.jpeg)


# 预处理器
1. 对解析完的语法树检查字段/表是否存在
2. 将SELECT中的*，扩展为表上的所有列


# 查询成本
一条SQL查询语句的成本有两块
## IO成本
数据都是存储在磁盘中的，将数据从磁盘中读取到内存中，这个过程所损耗的时间就是`IO成本`,在Mysql中，约定读取一个页的成本是`1.0`

## CPU成本
读取到数据就会进行对数据的一个运算，（验证是否满足条件，做一些分组排序的事情）；这些都是损耗CPU资源的，
这个过程所耗费的时间就是`CPU成本`,MySQL约定读取和检测一条数据是否符合条件的CPU成本是0.2

>我们需要从一个包含1w条数据的表中查询某些条件的数据，并且按照某个字段进行排序
> 
>规定`条件检测`的CPU成本=0.2,`排序`的CPU成本=1.0,<u>则对于每一条记录成本=1.2</u>,则对于1w条记录，<u>总的CPU成本=1.2*1w=1.2w</u>
> 
> IO成本规定一个数据页=1.0,每个数据页可以存放100条记录，则需要100个数据页,<u>总的IO成本=1.0*100=100</u>
> 
> 总成本开销=1.2W+100=1.21W

# 优化器
## 单表查询成本
### 找到可能使用的索引
经过预处理阶段后，需要为SQL查询语句制定一个执行计划
优化器会在表中有多个索引的情况下，基于查询的成本，决定选择哪一个索引
~~~sql
select * from product where id = 1
~~~
这个查询很简单，使用的就是主键索引
如果表中没有索引，就会全进行全表扫描

~~~sql
select id from product where id > 1  and name like 'i%';
~~~
因为这个SQL语句中包含两个索引，一个主键索引，一个普通索引，这个时候就需要优化器来决定这个时候，应该时候哪个索引（虽然有多个B+树，但是最后的叶子节点都是指向同一个地方）
在普通索引中查找效率更高，所以就不需要使用主键索引,

### 计算全表查询的成本
全表查询会计算将聚簇索引的所有页子节点数据页全部读取到内存中的开销，有多少的数据页就有多少的IO成本，每个记录都有0.2的CPU开销

表的统计信息
~~~sql
mysql> show table status like 'iam_user';
+----------+--------+---------+------------+-------+----------------+-------------+-----------------+--------------+-----------+--------------------+---------------------+---------------------+------------+-------------+----------+----------------+---------+
| Name     | Engine | Version | Row_format | Rows  | Avg_row_length | Data_length | Max_data_length | Index_length | Data_free | Auto_increment     | Create_time         | Update_time         | Check_time | Collation   | Checksum | Create_options | Comment |
+----------+--------+---------+------------+-------+----------------+-------------+-----------------+--------------+-----------+--------------------+---------------------+---------------------+------------+-------------+----------+----------------+---------+
| iam_user | InnoDB |      10 | Dynamic    | 28113 |            317 |     8929280 |               0 |      5816320 |   5242880 | 159332009760137304 | 2021-08-23 21:45:38 | 2021-08-23 21:44:30 | NULL       | utf8mb4_bin | NULL     |                |         |
+----------+--------+---------+------------+-------+----------------+-------------+-----------------+--------------+-----------+--------------------+---------------------+---------------------+------------+-------------+----------+----------------+---------+
1 row in set (0.02 sec)

~~~

* Row：当前表的记录数，这是一个估计值：28113
* Data_length:聚簇索引中数据页的字节大小,每个页16K，<u>8929280/1024/16=545</u>,所以这个表中可以查询的数据页数量=545

全表扫描的
`IO成本`：545*1.0=545,

`CPU成本`：28113*0.2=5622.6

`总成本` = 545+5622.6=`6167.6`

### 计算不同索引查询的成本
使用索引
1. 直接使用主键查询
2. 先使用使用二级索引查询索引数据和主键，再根据主键去聚簇索引中回表查询

xxxxxxxxxx package main​import (    "bufio"    "fmt"    "hash/fnv"    "io"    "os"    "strconv"    "strings")​type Record struct {    Key   string    Value string}​func main() {    // 读取小表S    s, err := readTable("s.txt")    if err != nil {        fmt.Println("读取小表S出错：", err)        return    }​    // 读取表B    b, err := readTable("b.txt")    if err != nil {        fmt.Println("读取表B出错：", err)        return    }​    // 第一步：判断是否可以进行内存哈希连接    if canMemoryHashJoin(s) {        result := memoryHashJoin(s, b)        fmt.Println(result)        return    }​    // 第二步：决定分区数    numPartitions := decideNumPartitions(s)​    // 第三步：读取小表S，进行哈希映射和创建哈希表    partitions := make([][]Record, numPartitions)    hashTables := make([]map[string]string, numPartitions)    for i := range partitions {        partitions[i] = []Record{}        hashTables[i] = make(map[string]string)    }    for _, record := range s {        partitionIndex := hash(record.Key) % numPartitions        partitions[partitionIndex] = append(partitions[partitionIndex], record)        hashTables[partitionIndex][record.Key] = record.Value    }​    // 第四步：建立位图向量    bitmaps := make([][]bool, numPartitions)    for i := range bitmaps {        bitmaps[i] = make([]bool, len(b))    }    for i, record := range b {        partitionIndex := hash(record.Key) % numPartitions        bitmaps[partitionIndex][i] = true    }​    // 第五步：如果内存不足，将分区写入磁盘    for i, partition := range partitions {        if len(partition)*2 > hashAreaSize() {            writePartition(i, partition)            partitions[i] = nil            hashTables[i] = nil        }    }​    // 第六步：读取小表S的剩余部分，重复第三步，直到读取完整个小表S    for _, record := range s {        if partitions[hash(record.Key)%numPartitions] != nil {            continue        }        partitionIndex := hash(record.Key) % numPartitions        partitions[partitionIndex] = append(partitions[partitionIndex], record)        hashTables[partitionIndex][record.Key] = record.Value        if len(partitions[partitionIndex])*2 > hashAreaSize() {            writePartition(partitionIndex, partitions[partitionIndex])            partitions[partitionIndex] = nil            hashTables[partitionIndex] = nil        }    }​    // 第七步：按大小对分区进行排序，选取多个分区建立哈希表    selectedPartitions := selectPartitions(partitions)​    // 第八步：根据哈希值建立哈希表    for _, partitionIndex := range selectedPartitions {        hashTable := make(map[string]string)        for _, record := range partitions[partitionIndex] {            hashTable[record.Key] = record.Value        }        hashTables[partitionIndex] = hashTable    }​    // 第九步：读取表B，使用位图向量进行过滤    filteredB := make([]Record, 0, len(b))    for i, record := range b {        partitionIndex := hash(record.Key) % numPartitions        if bitmaps[partitionIndex][i] {            filteredB = append(filteredB, record)        }    }​    // 第十步：将过滤后的数据映射到相应的分区，并计算哈希值    for _, record := range filteredB {        partitionIndex := hash(record.Key) % numPartitions        if hashTables[partitionIndex] == nil {            continue        }        value, ok := hashTables[partitionIndex][record.Key]        if ok {            result := join(record.Value, value)            writeResult(result)            continue        }        partitions[partitionIndex] = append(partitions[partitionIndex], record)        hashTables[partitionIndex][record.Key] = record.Value        if len(partitions[partitionIndex])*2 > hashAreaSize() {            writePartition(partitionIndex, partitions[partitionIndex])            partitions[partitionIndex] = nil            hashTables[partitionIndex] = nil            selectedPartitions = selectPartitions(partitions)            for _, partitionIndex := range selectedPartitions {                hashTable := make(map[string]string)                for _, record := range partitions[partitionIndex] {                    hashTable[record.Key] = record.Value                }                hashTables[partitionIndex] = hashTable            }            bitmaps = make([][]bool, numPartitions)            for i := range bitmaps {                bitmaps[i] = make([]bool, len(b))            }            for i, record := range b {                partitionIndex := hash(record.Key) % numPartitions                bitmaps[partitionIndex][i] = true            }            filteredB = make([]Record, 0, len(b))            for i, record := range b {                partitionIndex := hash(record.Key) % numPartitions                if bitmaps[partitionIndex][i] {                    filteredB = append(filteredB, record)                }            }            for _, record := range filteredB {                partitionIndex := hash(record.Key) % numPartitions                if hashTables[partitionIndex] == nil {                    continue                }                value, ok := hashTables[partitionIndex][record.Key]                if ok {                    result := join(record.Value, value)                    writeResult(result)                    continue                }                partitions[partitionIndex] = append(partitions[partitionIndex], record)                hashTables[partitionIndex][record.Key] = record.Value            }            continue        }    }​    // 第十三步：读取(Si,Bi)进行哈希连接，可能发生动态角色互换​    // 第十四步：如果分区后最小的分区仍大于内存，则进行嵌套循环哈希连接​    fmt.Println("算法执行完毕")}​func readTable(filename string) ([]Record, error) {    file, err := os.Open(filename)    if err != nil {        return nil, err    }    defer file.Close()​    reader := bufio.NewReader(file)​    var records []Record​    for {        line, err := reader.ReadString('\n')        if err != nil && err != io.EOF {            return nil, err        }        if line == "" && err == io.EOF {            break        }​        parts := strings.Split(strings.TrimSpace(line), ",")        if len(parts) != 2 {            return nil, fmt.Errorf("invalid format: %s", line)        }​        records = append(records, Record{Key: parts[0], Value: parts[1]})    }​    return records, nil}​func canMemoryHashJoin(s []Record) bool {    return len(s)*2 <= hashAreaSize()}​func memoryHashJoin(s []Record, b []Record) []string {    result := make([]string, 0)​    sMap := make(map[string]string)    for _, record := range s {        sMap[record.Key] = record.Value    }​    for _, record := range b {        if value, ok := sMap[record.Key]; ok {            result = append(result, join(record.Value, value))        }    }​    return result}​func decideNumPartitions(s []Record) int {    clusterSize := dbBlockSize() * hashMultiblockIOCount()    fAvailMem := 0.8 // 假设可用内存为总内存的80%    hashAreaSize := hashAreaSize()    return int(float64(hashAreaSize) / float64(clusterSize*fAvailMem))}​func hash(key string) int {    h := fnv.New32a()    h.Write([]byte(key))    return int(h.Sum32())}​func dbBlockSize() int64 {    return 8192 // 假设为8KB}​func hashMultiblockIOCount() int64 {    return 8 // 假设为8块IO操作同时进行}​func hashAreaSize() int {    return 1024 * 1024 * 1024 // 假设为1GB}​func writePartition(partitionIndex int, partition []Record) error {    filename := fmt.Sprintf("partition_%d.txt", partitionIndex)    file, err := os.Create(filename)    if err != nil {        return err    }    defer file.Close()​    writer := bufio.NewWriter(file)​    for _, record := range partition {        line := fmt.Sprintf("%s,%s\n", record.Key, record.Value)        _, err := writer.WriteString(line)        if err != nil {            return err        }    }​    return writer.Flush()}​func selectPartitions(partitions [][]Record) []int {    type partitionInfo struct {        index int        size  int    }​    infos := make([]partitionInfo, len(partitions))    for i, partition := range partitions {        infos[i].index = i        infos[i].size = len(partition)    }​    selectedInfos := make([]partitionInfo, 0)    sizeSum := 0​    for sizeSum < hashAreaSize()/2 && len(infos) > 0 {        maxInfoIndex := 0​        for i := 1; i < len(infos); i++ {            if infos[i].size > infos[maxInfoIndex].size {                maxInfoIndex = i            }        }​        selectedInfos = append(selectedInfos, infos[maxInfoIndex])        sizeSum += infos[maxInfoIndex].size​        infos[maxInfoIndex], infos[len(infos)-1] = infos[len(infos)-1], infos[maxInfoIndex]        infos = infos[:len(infos)-1]    }​    result := make([]int, len(selectedInfos))    for i, info := range selectedInfos {        result[i] = info.index    }​    return result}​func join(value1 string, value2 string) string {    return value1 + "," + value2 + "\n"}​func writeResult(result string) error {    file, err := os.OpenFile("result.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)    if err != nil {        return err    }    defer file.Close()​    writer := bufio.NewWriter(file)​    _, err = writer.WriteString(result)    if err != nil {        return err    }​    return writer.Flush()}go
* 范围区间数量

首先在二级索引中根据条件查数据的IO成本，一般是看查询条件涉及到几个范围，比如某个字段涉及到两个区间，那么就是两个范围，如果是等值查询就只有一个范围区间，

我们可以简单粗暴的理解`一个范围就是一个数据页`,IO成本都比较小，可能就是1*1.0;n*1.0,个位数级别

* 回表记录数量

二级索引的数据页读取到内存后，还需要根据条件去筛选数据，比如查询得到了10条数据，<u>CPU成本10*0.2=2</u>

拿到10条数据，还要回表到聚簇索引里面查询完整数据，这个时候就需要先估计`回表的IO成本`，认为1条数据就得回表查询聚簇索引的一个数据页，所以10条数据的IO成本就是<u>10*1.0=10</u>

拿到这10条完整的数据页，可能还需要根据条件判断计算CPU成本:10*0.2=2

总成本=1(二级索引一个范围的读取的数据页，IO成本)+2(二级索引查询到CPU成本)+10(10条记录的回表IO成本)+2（回表的CPU成本）=15


覆盖查询如果可以查询到数据的话，就应该避免回表查询

## 多表查询成本
数据库的多表查询采用的是`嵌套循环查询`的方法

对于左（外）连接和右（外）连接来说，驱动表固定，想要得到最优的查询方案只需要
* 分别为驱动表和被驱动表选择成本最低的访问方式
* 在内连接中，驱动表和被驱动表位置是可以互换的


### 找到成本最低的执行计划
计算出全表扫描，使用各个索引查询的成本之后，比对各个执行计划的成本，然后找出最低的执行计划


# 准备阶段
MySQL的优化器会认为我们写的SQL效率不高，就会对我们写的SQL语句进行优化，这个过程就叫做`查询重写`

## 子查询优化
* <u>相关子查询</u>:
  子查询中的执行依赖于外层查询的值，该子查询是嵌套在外部的查询的循环中，每次迭代都会执行一次子查询
   ~~~sql
  SELECT name
   FROM employees
   WHERE salary > (SELECT AVG(salary) FROM employees WHERE department = employees.department);
   ~~~
* <u>不相关子查询</u>:
  子查询可以单独运行出结果，不需要依赖于外层的查询,会执行一次，把查询的结果传递给外部查询
  ~~~sql
   SELECT name
   FROM employees
   WHERE salary > (SELECT AVG(salary) FROM employees);
  ~~~
   同时当前也可以改成JOIN查询
  ~~~sql
  SELECT e.name
   FROM employees e
   JOIN (SELECT AVG(salary) as avg_salary FROM employees) avg
   ON e.salary > avg.avg_salary;
  ~~~
  

<u>不相关子查询到效率比相关子查询的效率要高</u>

通常情况下`JOIN查询`比`子查询`要来的高效
- **优化器优化**：db通常会更好的优化 `JOIN`操作，选择更加高效的`执行计划`，包括选择正确的连接方式（内连接，外连接，自连接）,以及正确的使用索引 
- **只执行一次子查询**:在使用`JOIN`的情况下，只使用一次子查询，而后将结果存储在`内存或临时表`中，而不是在每一行外部查询上执行，




1. 将外连接转化成内连接
    > 将外连接转化成内连接可以提高查询的效率，内连接返回的是相应的行在两个表中同时满足，而外连接返回的符合条件在俩表中的所有行
    
    例如：
    ~~~sql
    SELECT o.order_id, c.name
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id;
    ~~~
    >这个外连接会将所有的符合条件的行都返回，即使name=nil，没有相应的顾客记录也被返回了
    
    然而
    ~~~sql
    SELECT o.order_id, c.name
    FROM orders o
    INNER JOIN customers c ON o.customer_id = c.customer_id;
    ~~~
    >这个sql会只返回匹配的record，nil不会被返回
   
   <u>同时如果给驱动表的where条件中添加上不为空的条件，其就等价为一个内连接,这样当驱动表为null的话即使主表成立，这条记录也无法返回</u>

   ~~~sql
   SELECT o.order_id, c.name
   FROM orders o
   LEFT JOIN customers c ON o.customer_id = c.customer_id WHERE c.customer_id is not null;
   ~~~
   



2. 将**EXIST** 转化成**IN** ，再将IN转化成**SEMI-JOIN**
   在子查询中，`exists`用来判断一个子查询是否返回有数据，`IN`判断数据是否存在在一系列值之间 exists只能使用子查询，必须先执行子查询，再执行其他操作
   ~~~SQL
   SELECT order_id
   FROM orders
   WHERE EXISTS (
   SELECT 1
   FROM customers
   WHERE orders.customer_id = customers.customer_id
   AND customers.name LIKE 'John%'
   );
   ~~~
   使用IN，来进行操作处理，可能会对子查询进行生成的数据进行`物化`,生成一个`物化表`,并且生成一个哈希索引,如果这个太大的话，就会磁盘化，转化成B+树索引
   使用IN，in可以通过join操作在外查询和子查询之间使用索引,缓存和其他技术来优化
   ~~~SQL
   SELECT orderid from order where customerid in 
   (select customerid from customer where order.customerid=customer.customerid
   and customer.name like 'john%'
   ~~~
   
   使用SEMI-JOIN;这样就可以避免生成物化表，而占用内存空间
   ~~~sql
   SELECT o.order_id
   FROM orders o
   INNER JOIN (
      SELECT DISTINCT order_id
      FROM order_items
      WHERE item_id = 100
   ) i ON o.order_id = i.order_id;
   ~~~



