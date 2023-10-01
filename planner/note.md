# 概念
B（s）->执行过程中访问了多少个区块
R（s）->执行后返回了多少条记录
V（s，F）->返回记录中字段包含不同值的数量
s对应的就是Scan对象
>比如有一张表的格式 student name age
> 返回了10条记录，
> 3条记录->age=18
> 4条记录->age=19
> 3条记录->age=20
> V(s,F)=3，因为对于age这个变量只有3个不同的值

对于上面这些数据，递归的调用这些scan对象的值

# 查询树
1. project->select->customer
这是一种查询树，对数据表customer先做select操作，把所有符合条件的行记录都抽取出来，再做project操作
把所由需要的列抽取出来


2. select->project->customer
这也是一种查询树，对customer先做project操作，把需要的列全部赛选出来，再根据条件，把符合条件的行记录赛选出来

不同查询树对数据操作的效率影响很大，所以我们需要构造出来所有可能的查询树后，计算不同的查询树的执行效率，选出最好的那个
这个步骤叫做`planning`

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

# 优化器
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

# 准备阶段
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
    
2. 将**EXIST** 转化成**IN** ，再将IN转化成**SEMIJOIN**
   在子查询中，exists用来判断一个子查询是否返回有数据，IN判断数据是否存在在一系列值之间
exists只能使用子查询，必须先执行子查询，再执行其他操作
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
使用IN，in可以通过join操作在外查询和子查询之间使用索引,缓存和其他技术来优化
~~~SQL
SELECT orderid from order where customerid in 
(select customerid from customer where order.customerid=customer.customerid
and customer.name like 'john%'
~~~

使用SEMIJOIN
~~~sql
SELECT o.order_id
FROM orders o
INNER JOIN (
   SELECT DISTINCT order_id
   FROM order_items
   WHERE item_id = 100
) i ON o.order_id = i.order_id;
~~~
# 逻辑变化
1. 否定消除：将多个表达式中的not，用得摩根去掉
2. 
