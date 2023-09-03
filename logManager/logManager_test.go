package logManager

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	fm "miniSQL/file_manager"
	"testing"
)

//生成一些日志内容分别写入string(“record1”)和int(1)这样的
//生成了一条完整的日志信息
func makeRecord(s string, n uint64) []byte {
	p := fm.NewPageBySize(1)           //开辟一个缓冲区
	npos := p.MaxLengthForString(s)    //获得s的在缓冲区存储占用的总长度
	b := make([]byte, npos+UINT64_LEN) //npos的就是把string数据写入到内部了，+8就是包含了后面的一个int
	p = fm.NewPageByBytes(b)           //把b的数据拷贝到p中
	p.SetString(0, s)                  //在缓冲区的0号偏移位置中写入s
	p.SetInt(npos, n)                  //在npos的位置处写入n
	return b
}

//start是日志的起始编号，end是日志的结束编号
func createRecord(lm *LogManager, start, end uint64) {
	for i := start; i <= end; i++ {
		rec := makeRecord(fmt.Sprintf("record%d", i), i) //rec就是成功在缓冲区中编码之后的日志了
		lm.Append(rec)                                   //将该日志数据添加进来
	}
}

func TestLogManager(t *testing.T) {
	fileManager, _ := fm.NewFileManager("/home/zevin/log_test", 400)
	logManager, err := NewLogManager(fileManager, "logfile")
	assert.Nil(t, err)
	createRecord(logManager, 1, 35)
	iter := logManager.Iterator()
	recNum := uint64(35) //从最新的数据开始进行遍历日志
	for iter.Valid() {
		rec := iter.Next()
		p := fm.NewPageByBytes(rec) //把读取的数据写入到缓冲区里面进行读取写入检查（字符串+整形）
		s := p.GetString(0)         //先读取日志中的字符串
		assert.Equal(t, fmt.Sprintf("record%d", recNum), s)
		npos := p.MaxLengthForString(s) //再读取日志中的整形
		val := p.GetInt(npos)
		assert.Equal(t, val, recNum)
		recNum--
	}
	createRecord(logManager, 36, 70)
	logManager.Flush()
	iter = logManager.Iterator()
	recNum = uint64(70)
	for iter.Valid() {
		rec := iter.Next()
		p := fm.NewPageByBytes(rec)
		s := p.GetString(0)
		assert.Equal(t, fmt.Sprintf("record%d", recNum), s)
		npos := p.MaxLengthForString(s)
		val := p.GetInt(npos)
		assert.Equal(t, val, recNum)
		recNum--
	}

}
