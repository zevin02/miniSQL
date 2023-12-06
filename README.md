<u>[英文](README-En.md)</u>
# 什么是 miniSQL？
miniSQL 是一个使用 Golang 实现的高性能关系型数据库。它旨在提供高效的数据查询，采用 B+ 树作为存储引擎，并基于 Volcano 模型构建执行器。它支持各种 SQL 操作，包括等值连接和常见的数据操作语句（CREATE、SELECT、DELETE、UPDATE）。此外，miniSQL 还提供对视图、索引和事务的支持，使用统一的接口设计以提高系统的灵活性和可维护性。

# 架构
[![ar.png](https://img1.imgtp.com/2023/10/21/kZ1UQDsp.png)](https://img1.imgtp.com/2023/10/21/kZ1UQDsp.png)


# 特点
- 支持关系代数操作，如 **SELECT**、**PROJECT** 和 **PRODUCT**。
- **解析器**：对 SQL 进行**词法**和**语法分析**，确保语义准确性。基于 **BNF** 语法构建 **SQL 语法树**，支持表达式、项、常量和谓词等上下文关系。
- **优化器**：通过评估 **IO** 和 **CPU** 成本的代数成本模型来重写和优化用户的 SQL 查询，使用动态规划算法获取最佳连接顺序，选择最优的查询树。 
- **连接操作符**：采用分区哈希连接算法进行快速数据连接。利用布隆过滤器减少不必要的查询开销。利用 Go 协程和单通道并发模型进行分区查询结果的整合。此外，还使用索引块嵌套循环连接算法优化小表连接，提高性能。
- **存储引擎**：采用 **B+** 树作为存储引擎，提供 CRUD 功能，并使用二分查找加速搜索。设计索引页和数据页，引入聚集索引和反向索引操作，并使用螃蟹算法进行并发控制，通过减小锁粒度并在路径上使用写锁来增强数据安全性。
- **文件管理器**：使用 **BlockId** 标识磁盘分区块，并使用 Page 在内存中管理块的 CRUD 操作。
- **日志管理器**：利用单个 Page 缓存日志，使用基于堆栈的方法进行数据写入和追踪可写位置。设计的迭代器从最新的日志开始迭代。
- **缓存管理器**：使用 **LRU** 算法管理缓存页面，通过热/冷列表区分数据类型。根据访问时间管理缓存分区，实现缓存预读和引用技术。利用脏页列表和空闲页列表管理脏页和空闲页，并通过超时机制解决死锁问题。
- **并发管理器**：实现 **S/X 锁**，使用管道信号进行同步和互斥。通过单例模式共享全局唯一锁表，并支持两阶段锁协议。
- **恢复管理器**：利用 **WAL** 技术进行撤消日志和重做日志，通过预写日志确保 ACID 特性。支持数据崩溃恢复所需的各种类型的日志信息。
- **元数据管理器**：
  - **视图管理**：记录当前视图名称及其对应的 SQL 创建语句。
  - **表管理**：使用字段表和表名表管理所有表的元数据。
  - **统计信息管理**：使用 **HyperLogLog** 计算特定时间内表中字段的基数，并在达到一定阈值时重新计算统计信息。
- **记录管理器**：利用模式管理表的字段信息，使用布局管理记录的元数据，包括偏移量和插槽大小。利用块中的特定插槽进行记录索引。


# 构建
# SQL 参考
## 数据类型
支持以下数据类型：

* INTEGER：64 位无符号整数，范围为 2^64-1
* VARCHAR：任意长度的字符串

## SQL 示例
支持的 SQL 语句示例：
~~~sql
// start transaction 
BEGIN;

//create table schema
CREATE TABLE PERSRON (
	PERSONID INT, 
	LASTNAME VARCHAR(255),
    FIRSTNAME VARCHAR(255),
    ADDRESS INT
);

//insert data
INSERT INTO employees (first_name, last_name, salary)
VALUES
('John', 'Doe', 50000);

INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country)
VALUES
(Cardinal, "Tom B. Erichsen", "Skagen 21", "Stavanger", 4006, "Norway");

//delete operation
DELETE FROM EMPLENT Where id=2;

DELETE FROM EMPLENT WHERE PERSON = 20 AND NAME = "STU";

//transaction rollback
ROLLBACK;

//update operation
UPDATE PERSON SET AGE = 20 WHERE NAME="NAME" AND PRED = 10;

//query operation
SELECT AGE,NAME FROM T;

SELECT AGE,NAME,DATE FROM T,B WHERE AGE = 1 AND TIME = "AGE" AND DATE =12;

//commit a transaction
COMMIT;
~~~
更新示例（索引、视图）：
~~~sql
//create a view
CREATE VIEW employeeview 
AS
SELECT employeeid, firstname, lastname, salary FROM employees WHERE salary = 50000;

CREATE VIEW Customer 
AS
SELECT CustomerName, ContactName FROM customers WHERE country="China";                                              
//create an index
CREATE INDEX indexName ON PERSON (LASTNAME,FIRST);
~~~

# Usage

# Tests
miniSQL 具有良好的测试覆盖率。其中包括许多低级组件的内部单元测试。

# Contact
* 请使用 [Github issues](https://github.com/zevin02/miniSQL/issues) 报告 bug.
* 可以通过电子邮件联系我：<u>zevin9427@gmail.com</u>.