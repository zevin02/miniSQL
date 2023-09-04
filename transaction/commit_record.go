package transaction

import (
	"fmt"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
)

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
