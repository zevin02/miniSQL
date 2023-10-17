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
> 1. 分区：将R和S按照连接的key进行分区，将R分成R1,R2,R3，将S分成S1,S2,S3
> 2. 构建：针对每个关系的分区，使用shj算法分别构建哈希表，对于每个分区使用相同的hash函数，并将记录插入到ri的哈希表中，对于s表也同样构建哈希表
> 3. probe：对于每个分区的RI，使用相同的哈希函数计算哈希值，再si的表中查找到匹配的记录