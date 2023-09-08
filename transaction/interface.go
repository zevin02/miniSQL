package transaction

import (
	"math"
	fm "miniSQL/file_manager"
)

//TransactionInterface 事务类型的接口
type TransactionInterface interface {
	Commit()
	RollBack() error
	Recover() error
	Pin(blk *fm.BlockId) error
	Unpin(blk *fm.BlockId) error
	GetInt(blk *fm.BlockId, offset uint64) (int64, error) //读取数据,直接让强转(int64)
	GetString(blk *fm.BlockId, offset uint64) (string, error)
	SetInt(blk *fm.BlockId, offset uint64, val int64, okToLog bool) error //是否需要产生日志
	SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool) error
	AvailableBuffer() uint64
	Size(filename string) (uint64, error)
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
	END_OF_FILE  = math.MaxUint32
)

//LogRecordInterface 各种日志类型
type LogRecordInterface interface {
	Op() RECORD_TYPE
	TxNumber() uint64
	Undo(tx TransactionInterface) //把当前的事务进行恢复
	ToString() string             //返回日志的文本形式
}
