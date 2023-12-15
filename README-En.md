
# What is miniSQL?

**MiniSQL** is crafted to create a high-performance relational database implemented in Golang. It strives to facilitate efficient data querying, employing **B+ trees** as the storage engine, and an executor based on the **Volcano Model**. It supports various SQL operations, including equi-join and common data manipulation statements ( **CREATE**, **SELECT**, **DELETE**, **UPDATE**,`JOIN`). Additionally, **miniSQL** provides support for `view`, `index`, and `transaction`, using a unified interface design for enhanced system flexibility and maintainability. Test coverage reaches 74%.

# Architecture

[![ar.png](https://img1.imgtp.com/2023/10/21/kZ1UQDsp.png)](https://img1.imgtp.com/2023/10/21/kZ1UQDsp.png)

# Features

- Support relational algebra operations such as `SELECT`, `PROJECT`, and `PRODUCT`,`JOIN`.
- **Parser**: Perform `lexical` and `syntax analysis` of SQL, ensuring semantic accuracy. Constructs the `SQL syntax tree` based on the <u>BNF grammar</u>, supporting context relationships such as `expressions`, `terms`, `constants`, and `predicates`.
- **Optimizer**: Rewrite and optimizes user SQL queries by evaluating the algebraic cost model for **IO** and **CPU** costs,use `dynamic programming algorithm` to obtain the best join order,selecting the optimal query tree.
- **Join Operator**: Implements the `Partition Hash Join algorithm` for fast data connections. Utilizes `Bloom Filter` to reduce unnecessary query overhead. Utilizes `Go Routine` and `single-channel` concurrent model for partition query result integration. Additionally, optimizes small table joins with the `Index Block-Nested-Loop-Join algorithm` for improved performance.
- **Storage Engine**: Utilizes `B+ tree` as the storage engine, providing CRUD functionality and accelerated searches with `binary search`. Designs index and data pages, introduces `clustering indexes` and `back-index` operations, and employs `crabbing algorithm` for concurrency control, enhancing data security by reducing lock granularity and employing write locks on the path.
- **File Manager**: Uses `BlockId` for disk partition block identification, and employs `Page` for managing block CRUD operations in memory.
- **Log Manager**: Utilizes a single Page for caching logs, employs a stack-based approach for data writing and tracking writable positions. The designed iterator starts iterating from the latest log.
- **Cache Manager**: Utilizes `LRU ` for cache page management, distinguishing data types through `hot/cold lists`. Manages cache partitions based on access time, implements cache `pre-reading` and `reference techniques`. Utilizes dirty page lists and free page lists for managing dirty and free pages, respectively, and resolves deadlock issues with a `timeout mechanism`.
- **Concurrency Manager**: Implements `S/X locks`, synchronizes and mutexes using pipeline signals. Shares global unique lock tables through the `singleton pattern`, and supports the `two-phase lock protocol`.
- **Recovery Manager**: Utilizes `WAL` technology for `undo log` and `redo log`, ensuring ACID properties with pre-written logs. Supports various types of log information required for data crash recovery.
- **Metadata Manager**:
  - **View Management**: Records current view names and corresponding SQL creation statements.
  - **Table Management**: Manages metadata for all tables using field tables and table name tables.
  - **Stat Management**: Uses `hyperloglog` to count the cardinality of a field in the table at a specific time, recalculating statistical information upon reaching a certain threshold.
- **Record Manager**: Utilizes schema for managing field information of tables, employs layout for managing metadata of records, including offsets and slot sizes. Utilizes specific slots in blocks for record indexing.


# build

# SQL Reference

## Data Types
The following data types are supported:
* **INTEGER**:64-bit unsigned integer numbers with a range of 2^64-1
* **VARCHAR**:string of any length


## SQL example
supported SQL statement
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

update example(<u>INDEX</u>,<u>VIEW</u>)
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
miniSQL has decent test coverage.These consist of in-code unit-tests for many low-level components


# Contact
* Please use [Github issues](https://github.com/zevin02/miniSQL/issues) for filling bugs.
* Reach me on <u>zevin9427@gmail.com</u>.