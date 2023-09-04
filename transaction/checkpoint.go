package transaction

import (
	"math"
	fm "miniSQL/file_manager"
	lg "miniSQL/logManager"
)

/*
	checkpoint表示在此之前的数据都已经成功写入到磁盘了，系统恢复的时候，就会从最近的checkpoint开始读取日志数据并恢复



*/

type CheckPointRecord struct {
}

func NewCheckPointRecord() *CheckPointRecord {
	return &CheckPointRecord{}
}

func (c *CheckPointRecord) Op() RECORD_TYPE {
	return CHECKPOINT
}

func (c *CheckPointRecord) TxNumber() uint64 {
	return math.MaxUint64 //它没有对应的交易号
}

func (c *CheckPointRecord) Undo() {
	//checkpoint没有什么可回滚的
}

func (c *CheckPointRecord) ToString() string {
	return "<CHECKPOINT>"
}

func WriteCheckPointToLog(lgmr *lg.LogManager) (uint64, error) {
	rec := make([]byte, UIN64_LENGTH)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, int64(CHECKPOINT)) //前8个字节写入他的日志类型为checkpoint
	return lgmr.Append(rec)
}
