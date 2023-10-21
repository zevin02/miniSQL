package transaction

import (
	"fmt"
	fm "miniSQL/file_manager"
	lg "miniSQL/log_manager"
)

/*
	<START, 3>
	<SETINT, 3, testfile, 1, 80, 2, 9999>
	<ROLLBACK, 3>
	这里启动了一个事务3,将testfile中的数据从2修改成了9999
	然后执行了一个回滚操作，这是我们可以从日志中发现，写入9999之前该位置的数据是2,于是我们就重新把9999的数据改成2,这样就完成了回滚操作

	rollback的操作步骤:
	1.获得要回滚的事务号
	2.从下往上读取日志，如果日志的事务号就是要回滚的事务号，就写入之前的数据；如果不是该事务号就忽略这个日志

*/
//回滚某个事务号
type RollBackRecord struct {
	tx_num uint64
}

func NewRollBackRecord(p *fm.Page) *RollBackRecord {
	return &RollBackRecord{
		tx_num: uint64(p.GetInt(UIN64_LENGTH)),
	}
}

func (r *RollBackRecord) Op() RECORD_TYPE {
	return ROLLBACK
}

func (r *RollBackRecord) TxNumber() uint64 {
	return r.tx_num
}

func (r *RollBackRecord) Undo(tx TransactionInterface) {
	//它没有回滚操作
	return
}

func (r *RollBackRecord) ToString() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.tx_num)
}

func WriteRollBackLog(lgmr *lg.LogManager, tx_num uint64) (uint64, error) {
	rec := make([]byte, 2*UIN64_LENGTH)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, int64(ROLLBACK))
	p.SetInt(UIN64_LENGTH, int64(tx_num))

	return lgmr.Append(rec)
}
