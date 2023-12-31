package transaction

import (
	"fmt"
	fm "miniSQL/file_manager"
	lm "miniSQL/log_manager"
)

//<SETSTRING, 2, testfile, 1, 40, one, one!>
//我们当前是在2号事务中，在testfile文件中的1号分区写入数据，原来是one，现在是one！
//但是我们实际上是分两条进行存储的,我们在实际写入的时候，先在日志中写入原先的数据，再写入修改之后的数据
//<SETSTRING, 2, testfile, 1, 40, one>(原先的)
//<SETSTRING, 2, testfile, 1, 40, one!>（之后的）
//我们在恢复的时候，从底部往上读取，这样先读到one！，写入，往上再读取到one,再继续写入同样位置，就可以覆盖掉之前的操作了，实现数据的恢复

type SetStringRecord struct {
	txNum  uint64      //当前事务对应的事务序列号
	offset uint64      //当前写入的偏移位置
	val    string      //当前写入的值
	blk    *fm.BlockId //当前文件在哪个分区中
}

//NewSetStringRecord 构造一个setstringrecord类型,初始化该日志对象
//p中记录了二进制的日志信息
func NewSetStringRecord(p *fm.Page) *SetStringRecord {
	//头8字节对应日志的类型,接下来的8字节对应的就是事务号,
	tpos := uint64(UIN64_LENGTH)                  //获得事务序列号是偏移位置
	txNum := p.GetInt(tpos)                       //先读取他的事务序列号
	fpos := tpos + UIN64_LENGTH                   //得到文件名的偏移位置
	filename := p.GetString(fpos)                 //得到文件名
	bpos := fpos + p.MaxLengthForString(filename) //得到区块号偏移
	blkNum := p.GetInt(bpos)                      //得到区块号
	offsetPos := bpos + UIN64_LENGTH              //得到当前区块某个位置的偏移
	offset := p.GetInt(offsetPos)                 //得到要操作的某个文件中某个块的偏移
	valPos := offsetPos + UIN64_LENGTH            //得到当前数据的偏移
	val := p.GetString(valPos)                    //得到日志中的数据
	return &SetStringRecord{
		txNum:  uint64(txNum),
		offset: uint64(offset),
		val:    val,
		blk:    fm.NewBlockId(filename, uint64(blkNum)), //开辟相应的位置信息
	}
}

//Op 获得此次的操作类型
func (s *SetStringRecord) Op() RECORD_TYPE {
	return SETSTRING
}

//TxNumber 获得事务的序列号
func (s *SetStringRecord) TxNumber() uint64 {
	return s.txNum
}

//Undo 把当前的事务进行恢复,回滚当前的操作，由于日志是从下往上遍历的，所以数据写入当前事务就相当于回滚,实际上就是把当前日志的数据写入到当前的事务中
func (s *SetStringRecord) Undo(tx TransactionInterface) {
	tx.Pin(s.blk)                               //占用当前的事务
	tx.SetString(s.blk, s.offset, s.val, false) //将原来字符串写回去,false说明当前操作不需要增加一个新的记录,往缓存中写入
	tx.Unpin(s.blk)                             //结束使用当前的事务
}

//ToString 返回日志的文本形式
func (s *SetStringRecord) ToString() string {
	str := fmt.Sprintf("<SETSTRING %d %s %d %d %s>", s.txNum, s.blk.FileName(), s.blk.Number(), s.offset, s.val)
	return str
}

//WriteSetStringLog 生成一个二进制的日志数据
//<SETSTRING, 2, testfile, 1, 40, one!>
//返回当前的日志序列号
func WriteSetStringLog(log *lm.LogManager, txNum uint64, blk *fm.BlockId, offset uint64, val string) (uint64, error) {
	tpos := uint64(UIN64_LENGTH) //获得事务序列号的位置
	fpos := tpos + UIN64_LENGTH  //获得文件名的位置
	p := fm.NewPageBySize(1)
	bpos := fpos + p.MaxLengthForString(blk.FileName()) //获得blk块的位置
	ops := bpos + UIN64_LENGTH                          //获得offset的位置
	vpos := ops + UIN64_LENGTH                          //获得val的位置
	recordLen := vpos + p.MaxLengthForString(val)       //获得整个日志的长度
	rec := make([]byte, recordLen)                      //这个record和page中是空想一个内存空间的
	p = fm.NewPageByBytes(rec)
	p.SetInt(0, int64(SETSTRING))       //写入日志类型
	p.SetInt(tpos, int64(txNum))        //写入事务序列号
	p.SetString(fpos, blk.FileName())   //写入文件名
	p.SetInt(bpos, int64(blk.Number())) //写入区块编号
	p.SetInt(ops, int64(offset))        //写入偏移量
	p.SetString(vpos, val)              //写入数据
	return log.Append(rec)              //追加到日志中
}
