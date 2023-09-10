package transaction

import (
	bm "miniSQL/buffer_manager"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
)

//RecoveryManager 该对象的产生一定伴随这一个事务的出现,同时也是由这个对象来产生事务中的日志信息
type RecoveryManager struct {
	logManager    *lm.LogManager    //当前日志管理器中记录了事务中的各种日志
	bufferManager *bm.BufferManager //这个buffer就是和当前事务的缓存管理器一样
	tx            *Transaction      //事务对象,里面由对应的事务序列号,就管理该事务即可
	txNum         int32             //当前事务对应的事务序列号
}

//NewRecoverManager 这个日志是全局日志,不只管理一个事务，二是管理一群事务的日志
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
	startRecord.WriteStartToLog() //将当前的数据，写成二进制形式的日志
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
	//将commit日志及之前的数据刷新到磁盘中（不仅包含这个事务的日志）
	err = r.logManager.FlushByLSN(lsn)
	if err != nil {
		return err
	} //把比当前日志小的数据都刷新到磁盘中
	return nil
}

//RollBack rollback是我们手动执行的，并不是系统执行的，我们由start，但是执行到一半，想要回滚当前操作
func (r *RecoveryManager) RollBack() error {
	r.doRollBack() //执行回滚操作
	//rollback的时候已经把数据写回到内存页中
	r.bufferManager.FlushAll(r.txNum)                           //把当前事务号在实现之前的数据全部冲刷到磁盘上
	lsn, err := WriteRollBackLog(r.logManager, uint64(r.txNum)) //写入日志，并获得日志号
	if err != nil {
		return nil
	}
	//把当前日志全部冲刷到磁盘中
	if err := r.logManager.FlushByLSN(lsn); err != nil {
		return err
	}
	return nil
}

//Recover 是系统执行的，发现由START，而没有找到commit，所以我们就需要执行数据的恢复,恢复到事务执行之前的状态4
//把每commit和rollback的事务都回滚
func (r *RecoveryManager) Recover() error {
	r.doRecover()                                  //同样也是执行回滚操作
	r.bufferManager.FlushAll(r.txNum)              //同样也是要把当前事务的之前的所有数据都刷新到磁盘中
	lsn, err := WriteCheckPointToLog(r.logManager) //放入一个checkpoint，表示当前系统执行了一次恢复操作，该行之前的记录没有用,当前写入会返回当前的日志号
	if err != nil {
		return err
	}
	//同样是要把当前日志到刷新到磁盘中
	err = r.logManager.FlushByLSN(lsn)
	if err != nil {
		return err
	}
	return nil
}

//SetInt 写入当前的日志，并且返回当前日志对应的区块号
func (r *RecoveryManager) SetInt(buff *bm.Buffer, offset uint64, _ int64) (uint64, error) {
	//先获得当前写入之前的数据
	oldVal := buff.Contents().GetInt(offset)
	blk := buff.Block()                                                       //获得当前buff对应的block信息
	return WriteSetIntLog(r.logManager, uint64(r.txNum), blk, offset, oldVal) //写入修改之前的日志
}

//SetString 写入当前的日志，并且返回当前日志对应的区块号
func (r *RecoveryManager) SetString(buff *bm.Buffer, offset uint64, _ string) (uint64, error) {
	//先获得当前写入之前的数据
	oldVal := buff.Contents().GetString(offset)
	blk := buff.Block()                                                          //获得当前buff对应的block信息
	return WriteSetStringLog(r.logManager, uint64(r.txNum), blk, offset, oldVal) //写入修改之前的日志
}

//CreateRecord 返回一个日志类型的接口
//传入数据的日志
func (r *RecoveryManager) CreateRecord(bytes []byte) LogRecordInterface {
	p := fm.NewPageByBytes(bytes) //放入缓冲区中
	//根据当前的日志类型，创建不同的日志对象
	switch RECORD_TYPE(p.GetInt(0)) {
	case CHECKPOINT:
		return NewCheckPointRecord()
	case START:
		return NewStartRecord(p, r.logManager)
	case COMMIT:
		return NewCommitRecord(p)
	case ROLLBACK:
		return NewRollBackRecord(p)
	case SETINT:
		return NewSetIntRecord(p)
	case SETSTRING:
		return NewSetStringRecord(p)
	default:
		panic("unknow log type")
	}
}

//doRollBack 这个rollback是针对某个具体的事务去执行
func (r *RecoveryManager) doRollBack() {
	iter := r.logManager.Iterator() //获得日志的迭代器，日志的迭代器是从最新的数据开始进行迭代，越往后面迭代的数据越旧,包含所有事务的日志
	for iter.Valid() {
		rec := iter.Next()               //获得当前的日志二进制的数据
		logRecord := r.CreateRecord(rec) //根据这个日志数据，创建一个日志对象
		//必须要保障是当前事务中写入的日志
		if logRecord.TxNumber() == uint64(r.txNum) {
			//如果不是当前事务的日志
			if logRecord.Op() == START {
				//如果找到了Start ，说明当前的回滚结束了，就可以返回了
				return
			}
			//说明还在当前的事务中，执行回滚即可
			logRecord.Undo(r.tx) //执行undo操作,undo的事务就是写入当前事务的缓冲区中
		}
	}
}

//doRecover 执行recover操作,这个recover是针对所有的事务去执行
func (r *RecoveryManager) doRecover() {
	//遍历日志管理器中的日志记录，查找所有的已经commit过了的事务的日志
	finishedTxs := make(map[uint64]bool) //set功能，集合的是所有执行过commit的操作的事务或者是执行了ROLLBACK的事务,把这些数据进行一个恢复日志的完整的数据
	iter := r.logManager.Iterator()
	for iter.Valid() {
		rec := iter.Next()
		logRecord := r.CreateRecord(rec)
		//由于是从最新的日志开始进行迭代，所以肯定事务肯定是先找到commit或者rollabck，其他的就是没完成

		if logRecord.Op() == COMMIT || logRecord.Op() == ROLLBACK {
			//记录当前有commit或者有rollback的日志（表示当前的日志已经完成了操作不需要进行恢复，要门已经commit提交刷新到磁盘，要门回滚恢复到了原来的状态，同样也刷新到磁盘了）
			finishedTxs[logRecord.TxNumber()] = true
		}

		exitst, ok := finishedTxs[uint64(r.txNum)]
		if !ok || !exitst {
			//走到这里这个说明他只有start，而没有commit和rollback，有头无尾的，就需要进行一个undo
			//把数据进行恢复
			logRecord.Undo(r.tx)
		}
	}
}
