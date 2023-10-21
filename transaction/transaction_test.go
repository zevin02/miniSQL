package transaction

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	bm "miniSQL/buffer_manager"
	fm "miniSQL/file_manager"
	lm "miniSQL/log_manager"
	"testing"
)

func TestTransaction(t *testing.T) {
	fmgr, err := fm.NewFileManager("/home/zevin/transaction_test", 400)
	lmgr, err := lm.NewLogManager(fmgr, "logfile")
	assert.Nil(t, err)
	bmgr := bm.NewBufferManager(fmgr, lmgr, 3) //开辟一个缓存管理器，内存池,供使用
	//创建一个事务
	tx1 := NewTransaction(fmgr, lmgr, bmgr) //存放入文件管理，日志管理，内存池管理
	blk := fm.NewBlockId("testfile", 1)     //创建一个文件分区，这个是实际写入的有效数据
	tx1.Pin(blk)                            //当前的事务把该文件块给占用了
	//一开始内存为随机值，因此不能进行日志记录,setint记录的日志是当前数据之前的数据
	assert.Equal(t, int32(1), tx1.txNum)
	tx1.SetInt(blk, 80, 1, false)
	tx1.SetString(blk, 40, "one", false)
	tx1.Commit() //将当前的事务进行提交 回滚操作会回滚到这个地方来

	tx2 := NewTransaction(fmgr, lmgr, bmgr)

	tx2.Pin(blk) //同样该事务操作的也是同样的文件块
	assert.Equal(t, int32(2), tx2.txNum)

	ival, _ := tx2.GetInt(blk, 80)
	sval, _ := tx2.GetString(blk, 40)
	assert.Equal(t, int64(1), ival)
	assert.Equal(t, "one", sval)
	fmt.Println("initial value at location 80 = ", ival)
	fmt.Println("initial value at location 40 = ", sval)
	new_ival := ival + 1
	new_sval := sval + "!"
	tx2.SetInt(blk, 80, new_ival, true)    //这次就需要写入日志
	tx2.SetString(blk, 40, new_sval, true) //这次就需要写入日志
	tx2.Commit()                           //尝试写入新的数据
	//开启第三个事务
	tx3 := NewTransaction(fmgr, lmgr, bmgr)
	tx3.Pin(blk) //同样该事务操作的也是同样的文件块
	assert.Equal(t, int32(3), tx3.txNum)
	ival, _ = tx3.GetInt(blk, 80)
	sval, _ = tx3.GetString(blk, 40)
	assert.Equal(t, int64(2), ival)
	assert.Equal(t, "one!", sval)
	fmt.Println("initial value at location 80 = ", ival)
	fmt.Println("initial value at location 40 = ", sval)
	tx3.SetInt(blk, 80, 999, true) //
	ival, _ = tx3.GetInt(blk, 80)
	assert.Equal(t, int64(999), ival)
	fmt.Println("initial value at location 80 = ", ival)
	//因为tx2没有调用commit，所以这个时候rollback就需要恢复到第一次commit时的数值
	//应为tx3也没有调用commit,所以这个tx3也被回滚了
	tx3.RollBack()

	tx4 := NewTransaction(fmgr, lmgr, bmgr) //再开辟一个事务
	tx4.Pin(blk)                            //继续占用这个文件块
	assert.Equal(t, int32(4), tx4.txNum)
	ival, _ = tx4.GetInt(blk, 80)
	fmt.Println("post-rollback at location 80 = ", ival)
	assert.Equal(t, int64(2), ival)
	tx4.Commit() //把当前的记录进行
}

func TestTransaction_AvailableBuffer(t *testing.T) {
	fmgr, err := fm.NewFileManager("/home/zevin/transaction_test", 400)
	lmgr, err := lm.NewLogManager(fmgr, "logfile")
	assert.Nil(t, err)
	bmgr := bm.NewBufferManager(fmgr, lmgr, 3)

	tx1 := NewTransaction(fmgr, lmgr, bmgr)

	availableBuffers := tx1.bufferManager.Avaliable()
	assert.Equal(t, uint32(3), availableBuffers)

	tx1.Commit()
}
