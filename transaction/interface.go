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
	UnPin(blk *fm.BlockId)
	GetInt(blk *fm.BlockId, offset uint64) uint64 //读取数据,直接让强转(int64)
	GetString(blk *fm.BlockId, offset uint64) string
	SetInt(blk *fm.BlockId, offset uint64, val uint64, okToLog bool) //是否需要产生日志
	SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool)
	AvailableBuffer() uint64
	Size(filename string) uint64
	Append(filename string) *fm.BlockId
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
)

type LogRecordInterface interface {
	Op() RECORD_TYPE
	TxNumber() uint64
	Undo(tx TransactionInterface) //把当前的事务进行恢复
	ToString() string             //返回日志的文本形式
}
