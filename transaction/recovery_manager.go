package transaction

import (
	bm "miniSQL/bufferManager"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
)

//RecoveryManager 该对象的产生一定伴随这一个事务的出现,同时也是由这个对象来产生事务中的日志信息
type RecoveryManager struct {
	logManager    *lm.LogManager
	bufferManager *bm.BufferManager
	tx            *Transaction //事务对象
	txNum         int32        //当前事务对应的事务序列号
}

func NewRecoverManager(tx *Transaction, TxNum int32, logManager *lm.LogManager, bufferManager *bm.BufferManager) *RecoveryManager {
	rm := &RecoveryManager{
		logManager:    logManager,
		bufferManager: bufferManager,
		txNum:         TxNum,
		tx:            tx,
	}
	//生成一条START日志,启动一个事务的时候，就说明一个事务启动了
	p := fm.NewPageBySize(32)
	p.SetInt(0, int64(START)) //写入日志类型
	p.SetInt(UIN64_LENGTH, int64(TxNum))
	startRecord := NewStartRecord(p, logManager)
	startRecord.WriteToLog() //将当前的数据，写成二进制形式的日志
	return rm
}

//Commit 当前恢复管理器将该事务全部提交，并生成一条日志信息
func (r *RecoveryManager) Commit() error {
	//先把数据都刷新到磁盘中，再写入日志，这个顺序是不能颠倒的（commit日志出现，就说明这个事务已经全部落盘了）
	r.bufferManager.FlushAll(r.txNum)                                //将当前的事务对应的所有数据都刷新到磁盘中
	lsn, err := WriteCommitkRecordLog(r.logManager, uint64(r.txNum)) //生成一条二进制的日志,写入日志缓冲区中
	if err != nil {
		return err
	}
	r.logManager.FlushByLSN(lsn) //把比当前日志小的数据都刷新到磁盘中

}
