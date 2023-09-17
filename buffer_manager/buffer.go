package buffer_manager

import (
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
)

//Page ([]byte)包装了缓冲区的中的数据,setint,getint,setstring,getstring

//Buffer(Page([]byte)),Pin(申请到一个页面的时候，增加一个计数,另一个组建也访问，计数+1),Unpin(页面用完的时候，计数减少,当这个页面的计数=0就可以释放这个页面了，重写回到磁盘上)

type Buffer struct {
	fileManager *fm.FileManager //文件管理器
	logManager  *lm.LogManager  //日志管理器(对数据库系统进行做崩溃恢复使用的)
	//当前的Buffer描述的就是page和blockid指向的数据
	contents *fm.Page    //缓存页面,装载BlockID指向的位置
	blk      *fm.BlockId //区块的描述
	pins     uint32      //当前buffer的引用计数,=0的时候就可以释放了
	txnum    int32       //当前buffer目前在管理事务号
	lsn      uint64      //当前buffer执行操作对应的日志号
}

func NewBuffer(fileManager *fm.FileManager, logManager *lm.LogManager) *Buffer {
	return &Buffer{
		fileManager: fileManager,
		logManager:  logManager,
		txnum:       -1, //当前暂时没用到
		lsn:         0,
		contents:    fm.NewPageBySize(fileManager.BlockSize()), //开辟一个缓存块
	}
}

//Contents 返回Buffer中包含的缓存数据
func (b *Buffer) Contents() *fm.Page {
	return b.contents
}

//Block 返回当前buffer指向的块数据信息
func (b *Buffer) Block() *fm.BlockId {
	return b.blk
}

//SetModify 如果上层组建引用buffer的人，修改了buffer中的数据，必须调用这个接口，告诉我们这个被修改了,这样就可以把数据写回到磁盘中
func (b *Buffer) SetModify(txnum int32, lsn uint64) {
	//调用这个接口的时候，我们就知道数据被修改了，如果只是读取数据的话，就不需要调用这个接口
	b.txnum = txnum //更新当前的事务
	if lsn > 0 {
		b.lsn = lsn //更新日志号
	}
}

//IsPinned 判断当前的缓存数据是否正在被使用，如果为true说明当前buffer还在被使用
//如果为false，说明当前的bufer已经没有人使用了
func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

//ModifyingTx 返回当前正在修改的事务号,当前正在执行的是哪个事务号
func (b *Buffer) ModifyingTx() int32 {
	return b.txnum
}

//Assign2Block 将指定BlockId数据从磁盘读取到缓存中,这个是普通没有从缓存读取的时候使用
func (b *Buffer) Assign2Block(blockId *fm.BlockId) {
	b.Flush()       //先将当前的缓存数据写入到磁盘中，避免数据的丢失
	b.blk = blockId //更新当前缓存指向的block块位置
	//在这个地方就会对数据进行修改
	b.fileManager.Read(b.blk, b.Contents()) //将blk的数据读取到Page缓存中
	b.pins = 0                              //当前是新读的一个page页面，所以引用计数为0
}

func (b *Buffer) Assign2BlockByCache(blockId *fm.BlockId) {
	b.logManager.FlushByLSN(b.lsn)           //把小于当前编号的日志都刷新到磁盘中,为以后系统的崩溃恢复提供支持
	b.fileManager.Write(b.blk, b.Contents()) //将已经修改好的数据写回到磁盘中
	b.txnum = -1
	b.blk = blockId                         //更新当前缓存指向的block块位置
	b.fileManager.Read(b.blk, b.Contents()) //将blk的数据读取到Page缓存中
	b.pins = 0                              //当前是新读的一个page页面，所以引用计数为0
}

//Flush 把数据刷新到磁盘中,先刷新日志，再刷新数据
func (b *Buffer) Flush() {
	if b.txnum > 0 { //当前的事务号！=0,说明当前就有新的数据增加进来
		b.logManager.FlushByLSN(b.lsn)           //把小于当前编号的日志都刷新到磁盘中,为以后系统的崩溃恢复提供支持
		b.fileManager.Write(b.blk, b.Contents()) //将已经修改好的数据写回到磁盘中
		b.txnum = -1                             //表示当前没有被修改过,后续可以重新使用这个buff来进行处理
	}
}

//Pin 增加引用计数
func (b *Buffer) Pin() {
	b.pins++
}

//Unpin 减小引用计数
func (b *Buffer) Unpin() {
	b.pins--
}

//Copy 对buffer实现一个拷贝构造
func (b *Buffer) Copy() *Buffer {
	// 创建一个新的 Buffer 对象
	newBuffer := &Buffer{
		fileManager: b.fileManager,
		logManager:  b.logManager,
		contents:    b.contents.Copy(), // 创建内容的深拷贝
		blk:         b.blk,
		pins:        b.pins,
		txnum:       b.txnum,
		lsn:         b.lsn,
	}
	return newBuffer
}
