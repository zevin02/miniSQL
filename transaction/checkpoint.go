package transaction

import (
	"math"
	fm "miniSQL/file_manager"
	lg "miniSQL/logManager"
)

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
	p.SetInt(0, uint64(CHECKPOINT)) //前8个字节写入他的日志类型为checkpoint
	return lgmr.Append(rec)
}
