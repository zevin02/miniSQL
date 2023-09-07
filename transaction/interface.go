package transaction

import (
	fm "miniSQL/file_manager"
)

//TransactionInterface 事务类型的接口
type TransactionInterface interface {
	Commit()
	RollBack()
	Recover()
	Pin(blk *fm.BlockId)
	Unpin(blk *fm.BlockId)
	GetInt(blk *fm.BlockId, offset uint64) (int64, error) //读取数据,直接让强转(int64)
	GetString(blk *fm.BlockId, offset uint64) (string, error)
	SetInt(blk *fm.BlockId, offset uint64, val int64, okToLog bool) error //是否需要产生日志
	SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool) error
	AvailableBuffer() uint64
	Size(filename string) uint64
	Append(filename string) (*fm.BlockId, error)
	BlockSize() uint64
}
type RECORD_TYPE uint64

const (
	CHECKPOINT RECORD_TYPE = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

const (
	UIN64_LENGTH = 8
	END_OF_FILE  = -1
)

//LogRecordInterface 各种日志类型
type LogRecordInterface interface {
	Op() RECORD_TYPE
	TxNumber() uint64
	Undo(tx TransactionInterface) //把当前的事务进行恢复
	ToString() string             //返回日志的文本形式
}
