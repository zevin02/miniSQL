# 数据库中的日志管理

在数据库中，有三种主要类型的日志，分别是：
* undo log（回滚日志）:innodb存储引擎层生成的日志，实现事务的原子性，使用在事务的回滚和MVCC
* redo log（重做日志) :innodb存储引擎中生成的日志，实现事务的持久性，掉电时可以进行故障恢复，重新播放日志，保证事务的一致性
* binlog(归档日志)：server层生成的日志，用于数据备份和主从复制

# undo log
在系统崩溃的时候，如何回滚到事务之前的状态呢
我们在事务执行的时候，都会记录下回滚的时候需要的信息到一个日志中，当前操作的数据，在事务执行之前的状态，在事务中途还没提交就崩溃的时候，就可以通过这个日志回滚到事务之前的状态
![](https://i.imgur.com/1QpFYA9.png)

每次在innodb引擎在对一条数据进行操作（crud）的时候，都需要把回滚需要的信息记录到undo log中
* **插入**一条新的数据的时候，记录下这条记录的主键值，在回滚的时候删除这个主键值对应的数据即可
  >~~~sql
  >INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (101, 'John', 'Doe', 50000.00);
  >~~~
  >我们插入的这条记录的主键值是101,在回滚的时候，删除这个101数据即可
* **删除**一条数据的时候，把这条记录的内容都记下来，回滚的时候，再将这些内容组成的记录插入即可
   ~~~sql
    DELETE FROM orders WHERE order_id = 123;
  ~~~
  在undo log中记录这条日志的信息
  ~~~text
    [Undo Log Entry]
    Transaction ID: 123456
    Table: orders
    Record ID: 123
    Column: order_id
    Old Value: 123
    Column: customer_name
    Old Value: 'John Doe'
    Column: order_total
    Old Value: 100.00
  ~~~
  
* **更新**一条记录的时候，记录下这个记录的旧值，在undo的时候，重新执行这个这个日志的操作即可

## undo log落盘
undo log实际上也是申请buffer pool中的数据页，所以刷盘的时机和数据页是一样的


# redo log

buffer pool会将数据缓存在内存中，但是这样可能导致断电的时候，数据就丢失了
为了防止断电数据丢失的情况，在记录需要更新的时候，innodb会更新内存（将这个页设置成脏页），并且将这次对页的修改，以redo log的形式保存起来，<u>这样才算更新成功</u>

之后Innodb会在合适的时间，后台线程把脏页刷新到磁盘中，`WAL技术`
![](https://i.imgur.com/n7Q68XX.png)
redo log的格式
>对 XXX 表空间中的 YYY 数据页 ZZZ 偏移量的地方做了AAA 更新

在事务提交的时候，只需要将redo log进行持久化即可，不需要立即将脏页进行持久化 

>undo页被修改的时候，也需要记录redo log

事务提交之前发生崩溃，重启之后，会使用undo log进行回滚事务，事务提交之后发生崩溃，会通过redo log来恢复事务
在事务启动的时候，redo log和undo log都被记录

## redo log刷盘
redo log并非直接写入磁盘的，这些会出现大量的IO操作，因此redo log也是有自己的buffer的，每产生一条redo log，会先写入到buffer中

刷盘的时机：
* 机器关闭的时候
* 当写入量打到redo log buffer内存空间的一半的时候，就会持久化到磁盘
  >每次的insert,update,delete操作都增加一个写入量，如果buffer的空间是1M，当写入量超过0.5M的时候，就会进行刷盘
* 后台线程会每隔1s，就将buffer刷新到磁盘中
* 可以配置，每次事务提交的时候，都将buffer持久化到磁盘中

redo log是循环写，两个redo log文件buffer，记录一个checkpoint，writepos，这个之间的数据都是可以被写入的
bin log是append only