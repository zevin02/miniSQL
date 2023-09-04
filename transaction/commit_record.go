package transaction

import (
	"fmt"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
)

/*
	所有系统启动的时候首先执行灾后恢复的工作，保持数据的一致性
	所有没有COMMIT的事务，都要保证写入的数据是事务执行之前的数据,所有执行了COMMIT的事务，都要保证数据是事务执行之后的数据
	但是出于效率考虑，执行了commit，但是部分写入的数据还在内存中，此时系统崩溃了，这部分内存中的数据就不会写入到磁盘中，系统会再某个时机将数据刷新到磁盘中
	但是在恢复管理器看来，只要日志中有COMMIT，那么当前事务就完成了，他不能保证事务写入的数据已经存在磁盘中了，所以恢复管理器可能会将已经完成的事务再执行一次

	执行事务的重新执行的功能：
	1.从头开始读取日志
	2.当遇到START X的时候，记下当前的事务序列号
	3.如果读取到setstring这类日志的时候，就执行对应的操作

	恢复管理器执行数据恢复的过程
	1.第一次扫描（自底向上）:确认哪些事务已经commit了，同时执行undo操作，撤回没有commit的执行的操作，并记录下已经执行commit的事务号
	2.第二次扫描（自顶向下）:从日志的开头重新扫描，读取START X，检查当前是否已经COMMIT（第一步执行的），如果是，就重新执行当前的事务，确保这些被重新使用，并要求缓存管理器立即将这些修改写入到磁盘中（当前的数据可能已经再磁盘上了，会降低系统效率）
	当前过程确保了已经commit的操作完全写入到了磁盘中，没有commit的操作执行undo恢复状态、

*/
type CommitRecord struct {
	txNum uint64 //事务号码
	//logManager *lm.LogManager //日志管理器
}

//NewCommitRecord 开头的8字节是对应的日志的类型，接下来的8字节对应事务序列号
//直接从缓存页面中获得事务号
func NewCommitRecord(page *fm.Page, logManager *lm.LogManager) *CommitRecord {
	txNum := page.GetInt(UIN64_LENGTH) //从缓存中读取事务序列号,因为前8字节是当前日志的类型
	return &CommitRecord{
		txNum: txNum,
	}
}

//Op 返回当前的操作码
func (c *CommitRecord) Op() RECORD_TYPE {
	return COMMIT
}

//TxNumber 返回当前的事务序列号
func (c *CommitRecord) TxNumber() uint64 {
	return c.txNum
}

//Undo 回滚当前的操作
func (c *CommitRecord) Undo() {
	//Commit没什么好回滚的操作
}

//ToString 将当前的日志按照文本的形式返回
func (c *CommitRecord) ToString() string {
	str := fmt.Sprintf("<COMMIT %d>", c.txNum)
	return str
}

//WriteCommitkRecordLog 将commit数据写入到日志文本中
func WriteCommitkRecordLog(lgmr *lm.LogManager, tx_num uint64) (uint64, error) {
	rec := make([]byte, 2*UIN64_LENGTH)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(COMMIT))
	p.SetInt(UIN64_LENGTH, tx_num)

	return lgmr.Append(rec)
}
