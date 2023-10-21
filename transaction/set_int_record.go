package transaction

import (
	"fmt"
	fm "miniSQL/file_manager"
	lm "miniSQL/log_manager"
)

//<SETSTRING, 2, testfile, 1, 40, one, one!>
//我们当前是在2号事务中，在testfile文件中的1号分区写入数据，原来是one，现在是one！
//但是我们实际上是分两条进行存储的
//<SETSTRING, 2, testfile, 1, 40, one>
//<SETSTRING, 2, testfile, 1, 40, one!>
//我们在恢复的时候，从底部往上读取，这样先读到one！，写入，往上再读取到one,再继续写入同样位置，就可以覆盖掉之前的操作了，实现数据的恢复

//由于我们实现的这个日志，记录的都是之前的数据，所以undo就是把这个日志的数据重新写入到之前日志块中

type SetIntRecord struct {
	txNum  uint64      //当前事务对应的事务序列号
	offset uint64      //当前写入的偏移位置
	val    int64       //当前写入的值
	blk    *fm.BlockId //当前文件在哪个分区中
}

//NewSetStringRecord 构造一个setstringrecord类型,初始化该日志对象
//p中记录了二进制的日志信息

func NewSetIntRecord(p *fm.Page) *SetIntRecord {
	//头8字节对应日志的类型,接下来的8字节对应的就是事务号,
	tpos := uint64(UIN64_LENGTH)                  //获得事务序列号是偏移位置
	txNum := p.GetInt(tpos)                       //先读取他的事务序列号
	fpos := tpos + UIN64_LENGTH                   //得到文件名的偏移位置
	filename := p.GetString(fpos)                 //得到文件名
	bpos := fpos + p.MaxLengthForString(filename) //得到区块号偏移
	blkNum := p.GetInt(bpos)                      //得到区块号
	offsetPos := bpos + UIN64_LENGTH              //得到当前区块某个位置的偏移
	offset := p.GetInt(offsetPos)                 //得到要操作的某个块的偏移
	valPos := offsetPos + UIN64_LENGTH            //得到当前数据的偏移
	val := p.GetInt(valPos)                       //得到日志中的数据
	return &SetIntRecord{
		txNum:  uint64(txNum),
		offset: uint64(offset),
		val:    val,
		blk:    fm.NewBlockId(filename, uint64(blkNum)), //开辟相应的位置信息
	}
}

//Op 获得此次的操作类型
func (s *SetIntRecord) Op() RECORD_TYPE {
	return SETINT
}

//TxNumber 获得事务的序列号
func (s *SetIntRecord) TxNumber() uint64 {
	return s.txNum
}

//Undo 把当前的事务进行恢复,回滚当前的操作
func (s *SetIntRecord) Undo(tx TransactionInterface) {
	tx.Pin(s.blk)
	tx.SetInt(s.blk, s.offset, s.val, false) //将原来字符串写回去,false说明当前操作不需要增加一个新的记录,往缓存中写入
	tx.Unpin(s.blk)
}

//ToString 返回日志的文本形式
func (s *SetIntRecord) ToString() string {
	str := fmt.Sprintf("<SETINT %d %s %d %d %d>", s.txNum, s.blk.FileName(), s.blk.Number(), s.offset, s.val)
	return str
}

//WriteSetStringLog 生成一个二进制的日志数据
//<SETSTRING, 2, testfile, 1, 40, one!>
//返回当前的日志序列号
func WriteSetIntLog(log *lm.LogManager, txNum uint64, blk *fm.BlockId, offset uint64, val int64) (uint64, error) {
	tpos := uint64(UIN64_LENGTH) //获得事务序列号的位置
	fpos := tpos + UIN64_LENGTH  //获得文件名的位置
	p := fm.NewPageBySize(1)
	bpos := fpos + p.MaxLengthForString(blk.FileName()) //获得blk块的位置
	ops := bpos + UIN64_LENGTH                          //获得offset的位置
	vpos := ops + UIN64_LENGTH                          //获得val的位置
	recordLen := vpos + UIN64_LENGTH                    //获得整个日志的长度
	rec := make([]byte, recordLen)                      //这个record和page中是空想一个内存空间的
	p = fm.NewPageByBytes(rec)
	p.SetInt(0, int64(SETINT))          //写入日志类型
	p.SetInt(tpos, int64(txNum))        //写入事务序列号
	p.SetString(fpos, blk.FileName())   //写入文件名
	p.SetInt(bpos, int64(blk.Number())) //写入区块编号
	p.SetInt(ops, int64(offset))        //写入偏移量
	p.SetInt(vpos, int64(val))          //写入数据
	return log.Append(rec)              //每次写入都会增加日志号
}
