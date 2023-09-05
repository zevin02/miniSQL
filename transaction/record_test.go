package transaction

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
	"testing"
)

func TestStartRecord(t *testing.T) {
	fileManager, err := fm.NewFileManager("/home/zevin/start_test", 400) //打开一个文件管理器来管理文件
	assert.Nil(t, err)
	logManager, err := lm.NewLogManager(fileManager, "record_file")
	assert.Nil(t, err)
	txNum := int64(13) //设置的事务序列号是13
	//将日志按照二进制形式写入到page中
	p := fm.NewPageBySize(32)                    //开辟一个缓存块
	p.SetInt(0, int64(START))                    //先写入一个STRART
	p.SetInt(UIN64_LENGTH, txNum)                //写入一个事务序列号
	startRecord := NewStartRecord(p, logManager) //
	expectedStr := fmt.Sprintf("<START %d>", txNum)
	assert.Equal(t, expectedStr, startRecord.ToString())
	_, err = startRecord.WriteStartToLog() //把当前数据按照二进制新是写入到磁盘中
	assert.Nil(t, err)
	iter := logManager.Iterator()
	rec := iter.Next()
	recOp := binary.LittleEndian.Uint64(rec[0:8]) //从record中提取当前的日志类型
	recTxNum := binary.LittleEndian.Uint64(rec[8:len(rec)])
	assert.Equal(t, recOp, uint64(START))
	assert.Equal(t, int64(recTxNum), txNum)
}

func TestSetStringRecord(t *testing.T) {
	fmgr, err := fm.NewFileManager("/home/zevin/setstring_test", 400)
	assert.Nil(t, err)
	lmgr, err := lm.NewLogManager(fmgr, "setstring")
	assert.Nil(t, err)
	str := "origin string"                      //写入的字符串
	blk := uint64(1)                            //写入的区块编号
	dummy_blk := fm.NewBlockId("dummy_id", blk) //生成一个区块管理器
	txNum := uint64(1)                          //生成事务序列号
	offset := uint64(13)                        //生成偏移量
	//写入用来恢复的日志
	WriteSetStringLog(lmgr, txNum, dummy_blk, offset, str) //将日志数据写入，生成一个日志
	//pp中就是我们用户实际要写入的记录的数据
	pp := fm.NewPageBySize(400)           //生成一个缓存区块
	pp.SetString(offset, str)             //在当前位置写入当前的数据
	iter := lmgr.Iterator()               //获得迭代器
	rec := iter.Next()                    //进行迭代,获得数据的日志数据
	logp := fm.NewPageByBytes(rec)        //将获得的日志数据写入缓冲区
	setstrRec := NewSetStringRecord(logp) //将当前的缓存数据写入的奥setstring的管理器
	expectRec := fmt.Sprintf("<SETSTRING %d %s %d %d %s>", txNum, dummy_blk.FileName(), blk, offset, str)
	assert.Equal(t, setstrRec.ToString(), expectRec)

	//对数据进行修改
	pp.SetString(offset, "modify string 1")
	//修改了第二次
	pp.SetString(offset, "modify string 2")

	txStub := NewTxStub(pp)            //创建一个事务对象
	setstrRec.Undo(txStub)             //将数据进行恢复，回滚undo
	recoverStr := pp.GetString(offset) //恢复原来的数据
	assert.Equal(t, recoverStr, str)   //和最开始的数据要求一致
}

//测试SetInt
func TestSetIntRecord(t *testing.T) {
	fmgr, err := fm.NewFileManager("/home/zevin/setint_test", 400)
	assert.Nil(t, err)
	lmgr, err := lm.NewLogManager(fmgr, "setint")
	assert.Nil(t, err)
	val := int64(11)                            //写入的字符串
	blk := uint64(1)                            //写入的区块编号
	dummy_blk := fm.NewBlockId("dummy_id", blk) //生成一个区块管理器
	txNum := uint64(1)                          //生成事务序列号
	offset := uint64(13)                        //生成偏移量
	//写入用来恢复的日志
	WriteSetIntLog(lmgr, txNum, dummy_blk, offset, val) //将日志数据写入，生成一个日志
	//pp中就是我们用户实际要写入的记录的数据
	pp := fm.NewPageBySize(400)        //生成一个缓存区块
	pp.SetInt(offset, val)             //在当前位置写入当前的数据
	iter := lmgr.Iterator()            //获得迭代器
	rec := iter.Next()                 //进行迭代,获得数据的日志数据
	logp := fm.NewPageByBytes(rec)     //将获得的日志数据写入缓冲区
	setstrRec := NewSetIntRecord(logp) //将当前的缓存数据写入的奥setstring的管理器
	expectRec := fmt.Sprintf("<SETINT %d %s %d %d %d>", txNum, dummy_blk.FileName(), blk, offset, val)
	assert.Equal(t, setstrRec.ToString(), expectRec)

	//对数据进行修改
	pp.SetInt(offset, int64(33))
	//修改了第二次
	pp.SetInt(offset, int64(98))

	txStub := NewTxStub(pp)          //创建一个事务对象
	setstrRec.Undo(txStub)           //将数据进行恢复，回滚undo
	recoverVal := pp.GetInt(offset)  //恢复原来的数据
	assert.Equal(t, recoverVal, val) //和最开始的数据要求一致
}

//检查commit的日志信息
func TestCommitRecord(t *testing.T) {
	fmgr, err := fm.NewFileManager("/home/zevin/commit_test", 400)
	assert.Nil(t, err)
	lmgr, err := lm.NewLogManager(fmgr, "commit")
	assert.Nil(t, err)
	txNum := uint64(12)
	WriteCommitkRecordLog(lmgr, txNum) //将数据写入日志
	iter := lmgr.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec) //将rec的数据写入到缓存中
	rollbackrec := NewCommitRecord(pp)
	expected_val := fmt.Sprintf("<COMMIT %d>", txNum) //实际我们需要的日志
	assert.Equal(t, expected_val, rollbackrec.ToString())
}

func TestCheckPointRecord(t *testing.T) {
	fmgr, err := fm.NewFileManager("/home/zevin/checkPoint_test", 400)
	assert.Nil(t, err)
	lmgr, err := lm.NewLogManager(fmgr, "checkpoint")
	assert.Nil(t, err)
	WriteCheckPointToLog(lmgr)
	iter := lmgr.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)
	val := pp.GetInt(0)
	assert.Equal(t, uint64(val), uint64(CHECKPOINT))
	checkPointRec := NewCheckPointRecord()
	expectVal := "<CHECKPOINT>"
	assert.Equal(t, expectVal, checkPointRec.ToString())
}

func TestRollBackRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("/home/zevin/rollback_test", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "rollback")
	tx_num := uint64(13)
	WriteRollBackLog(log_manager, tx_num)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)

	roll_back_rec := NewRollBackRecord(pp)
	expected_str := fmt.Sprintf("<ROLLBACK %d>", tx_num)

	require.Equal(t, expected_str, roll_back_rec.ToString())
}
