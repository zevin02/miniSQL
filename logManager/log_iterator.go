package logManager

import (
	fm "miniSQL/file_manager"
)

/*
	logIterator用于遍历区块中的日志，日志是从底部开始往上写的，遍历是从上往下读取的
	如果当前的区块记录的日志编号是1,2，3,4，存储的顺序就是4,3,2,1
	日志遍历器读取的顺序也就是4,3,2,1
*/

type LogIterator struct {
	fileManager *fm.FileManager //文件管理器读取数据管理
	blk         *fm.BlockId     //指定当前数据所在的区块
	p           *fm.Page        //数据的缓冲块
	currentPos  uint64          //当前遍历的偏移
	boundary    uint64          //数据的下界
}

func NewLogIterator(file *fm.FileManager, blk *fm.BlockId) *LogIterator {
	it := &LogIterator{
		fileManager: file,
		blk:         blk,
	}
	//读取给定区块的数据,把数据先从二进制文件中读取并写入到缓冲区中
	it.p = fm.NewPageBySize(file.BlockSize()) //开辟一个缓冲区
	err := it.moveToBlock(blk)                //把blk区块的数据读取到缓冲区中，数据的头8字节记录了当前走到了哪里,
	if err != nil {
		return nil
	}
	return it
}

//moveToBlock 读取磁盘中的一个数据块到缓冲区中
func (it *LogIterator) moveToBlock(blk *fm.BlockId) error {
	//将对应区块的数据从磁盘读取到内存中
	_, err := it.fileManager.Read(blk, it.p)
	if err != nil {
		return err
	}
	//获得日志起始的地址
	it.boundary = uint64(it.p.GetInt(0)) //从缓冲区中读取日志下一次要写入的边界，实际上就是当前日志的末尾

	it.currentPos = it.boundary //可以从这个位置开始进行读取，遍历数据
	return nil
}

//Next 遍历得到下一条日志
func (it *LogIterator) Next() []byte {
	//会先读取编号最大的数据
	if it.currentPos == it.fileManager.BlockSize() {
		//当前的位置已经到了区块的末尾了，说明当前区块的数据已经读取完成了
		//我们就需要开辟一个新的区块，就是当前区块的前一个区块，继续将数据读取上来
		it.blk = fm.NewBlockId(it.blk.FileName(), it.blk.Number()-1) //获得的是前一个区块的数据
		it.moveToBlock(it.blk)                                       //将数据读取到缓冲区中
	}
	record := it.p.GetBytes(it.currentPos)            //从缓冲区中读取数据
	it.currentPos += UINT64_LEN + uint64(len(record)) //下一次读取的位置就是当前位置+8再加上当前数据的长度
	return record

}

func (it *LogIterator) Valid() bool {
	/*
		如果当前区块数据全部读完，但是区块号不是0,说明还有其他数据可以读取
	*/
	return it.currentPos < it.fileManager.BlockSize() || it.blk.Number() > 0
}
