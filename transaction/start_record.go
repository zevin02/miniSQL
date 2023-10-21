package transaction

import (
	"fmt"
	fm "miniSQL/file_manager"
	lm "miniSQL/log_manager"
)

type StartRecord struct {
	txNum      uint64         //事务号码
	logManager *lm.LogManager //日志管理器
}

//NewStartRecord 开头的8字节是对应的日志的类型，接下来的8字节对应事务序列号
//page中已经有数据了
func NewStartRecord(page *fm.Page, logManager *lm.LogManager) *StartRecord {
	txNum := page.GetInt(UIN64_LENGTH) //从缓存中读取事务序列号,因为前8字节是当前日志的类型
	return &StartRecord{
		txNum:      uint64(txNum),
		logManager: logManager,
	}
}

//Op 返回当前的操作码
func (s *StartRecord) Op() RECORD_TYPE {
	return START
}

//TxNumber 返回当前的事务序列号
func (s *StartRecord) TxNumber() uint64 {
	return s.txNum
}

func (s *StartRecord) Undo(TransactionInterface) {
	//Start没什么好undo的
	return
}

//ToString 将当前的日志按照文本的形式返回
func (s *StartRecord) ToString() string {
	str := fmt.Sprintf("<START %d>", s.txNum)
	return str
}

//WriteToLog 将数据写入到文本中
func (s *StartRecord) WriteStartToLog() (uint64, error) {
	record := make([]byte, 2*UIN64_LENGTH) //一共是16个字节
	p := fm.NewPageByBytes(record)         //构造一个buffer出来
	p.SetInt(uint64(0), int64(START))      //前面8字节是日志的类型
	p.SetInt(UIN64_LENGTH, int64(s.txNum)) //后面8字节是日志的事务序列号
	return s.logManager.Append(record)     //把数据写入到日志中

}
