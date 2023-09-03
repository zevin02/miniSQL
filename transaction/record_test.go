package transaction

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
	"testing"
)

func TestStartRecord(t *testing.T) {
	fileManager, err := fm.NewFileManager("/home/zevin/buffer_test", 400) //打开一个文件管理器来管理文件
	assert.Nil(t, err)
	logManager, err := lm.NewLogManager(fileManager, "record_file")
	assert.Nil(t, err)
	txNum := uint64(13) //设置的事务序列号是13
	//将日志按照二进制形式写入到page中
	p := fm.NewPageBySize(32)                    //开辟一个缓存块
	p.SetInt(0, uint64(START))                   //先写入一个STRART
	p.SetInt(UIN64_LENGTH, txNum)                //写入一个事务序列号
	startRecord := NewStartRecord(p, logManager) //
	expectedStr := fmt.Sprintf("<START %d>", txNum)
	assert.Equal(t, expectedStr, startRecord.ToString())
	_, err = startRecord.WriteToLog() //把当前数据按照二进制新是写入到磁盘中
	assert.Nil(t, err)
	iter := logManager.Iterator()
	rec := iter.Next()
	recOp := binary.LittleEndian.Uint64(rec[0:8]) //从record中提取当前的日志类型
	recTxNum := binary.LittleEndian.Uint64(rec[8:len(rec)])
	assert.Equal(t, recOp, uint64(START))
	assert.Equal(t, recTxNum, txNum)
}
