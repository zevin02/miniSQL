package transaction

import (
	"errors"
	"fmt"
	bm "miniSQL/bufferManager"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
	"sync"
)

var txNumMu sync.Mutex     //事务的锁
var nextTxNumId = int32(0) //事务的序列号

//获得下一个事务的id
func nextTxNum() int32 {
	txNumMu.Lock()
	defer txNumMu.Unlock()
	nextTxNumId++
	return nextTxNumId
}

type Transaction struct {
	myBuffers      *BufferList        //管理当前被pin的buffer对象
	logManager     *lm.LogManager     //日志管理
	fileManager    *fm.FileManager    //文件管理
	recoverManager *rm.RecoverManager //恢复管理器,用于事务恢复或者回滚
	txNum          int32              //当前的事务序列号
	bufferManager  *bm.BufferManager  //缓存管理器
	concurrentMgr  ConcurrentManager  //管理并发请求
}

//NewTransaction 构造一个事务对象
func NewTransaction(fileManager *fm.FileManager, logManager *lm.LogManager, bufferManager *bm.BufferManager) *Transaction {
	tx := &Transaction{
		fileManager:   fileManager,
		logManager:    logManager,
		bufferManager: bufferManager,
		myBuffers:     NewBufferList(bufferManager), //构造一个bufferLsit对象，传入对应的缓存管理器即可
		txNum:         nextTxNum(),
	}
	//创建同步管理器
	//创建恢复管理器
	tx.recoverManager = NewRecoverManager(tx, tx.txNum, logManager, bufferManager)
	return tx
}

//Commit 将当前的事务进行提交
func (t *Transaction) Commit() {
	t.recoverManager.Commit()
	r := fmt.Sprintf("transaction %d commited", t.txNum)
	fmt.Println(r)
	//TODO 释放同步管理器
	//执行commit之后，当前事务就全部完成了，所有的数据都会写入到磁盘中去，将当前用于存储当前缓存页全部进行解锁，解引用
	t.myBuffers.UnpinAll()
}

//RollBack 执行一个回滚操作,好像当前的所有事务没有发生一样,丢弃当前事务
func (t *Transaction) RollBack() {
	t.recoverManager.RollBack()
	r := fmt.Sprintf("transaction %d roll back", t.txNum)
	fmt.Println(r)

	//同样也是要释放同步管理器
	t.myBuffers.UnpinAll() //释放当前事务的所有缓存页
}

//Recover 系统启动的时候，会在所有事务执行前，运行该函数
//系统启动的时候发现上一次的事务在执行到一半的时候，发生崩溃或者断电了，数据写到一半，启动之后，就要将写到一半的数据给抹掉，恢复到写入之前的状态
func (t *Transaction) Recover() {
	t.bufferManager.FlushAll(t.txNum) //把当前事务还没有处理完的数据全部写入到磁盘
	t.recoverManager.Recover()
}

func (t *Transaction) Pin(blk *fm.BlockId) {
	t.myBuffers.Pin(blk) //调用pin进行管理
}

func (t *Transaction) Unpin(blk *fm.BlockId) {
	t.myBuffers.Unpin(blk) //调用pin进行管理
}

//缓存不存在
func (t *Transaction) bufferNoExist(blk *fm.BlockId) error {
	err_s := fmt.Sprintf("No Buffer found  for given blk %d with filename: %s\n", blk.Number(), blk.FileName())
	err := errors.New(err_s)
	return err
}

//在事务中读取一个数据
//读取数据,直接让强转(int64)
func (t *Transaction) GetInt(blk *fm.BlockId, offset uint64) (int64, error) {
	//读取数据的时候，调用同步管理器加s类型的锁,加锁的数据范围要尽可能小
	//t.slock()//加锁没有释放
	buff := t.myBuffers.getBuf(blk)
	if buff == nil {
		//当前区块的数据并不存在
		return -1, t.bufferNoExist(blk)
	}
	return buff.Contents().GetInt(offset), nil //在指定的buff中获得相应的数据

}

func (t *Transaction) GetString(blk *fm.BlockId, offset uint64) (string, error) {
	//调用同步管理器加s锁
	buff := t.myBuffers.getBuf(blk)
	if buff == nil {
		//当前区块的数据并不存在
		return "", t.bufferNoExist(blk)
	}
	return buff.Contents().GetString(offset), nil //从相应的地方获得指定的数据
}

//okToLog=true会生成记录，为false就不会生成对应的记录
func (t *Transaction) SetInt(blk *fm.BlockId, offset uint64, val int64, okToLog bool) error {
	//调用同步管理器的x锁
	//t.xlock()
	buff := t.myBuffers.getBuf(blk)
	if buff == nil {
		//当前区块的数据并不存在
		return t.bufferNoExist(blk)
	}
	//把当前操作作为一个日志记录起来
	var lsn uint64
	var err error
	if okToLog {
		//生成记录
		lsn, err = t.recoverManager.SetInt(buff, offset, val) //转发给recovermanager，由他在里面增加这个记录,毕竟是由他来恢复的
		if err != nil {
			return err
		}
	}
	p := buff.Contents()  //拿到他的缓存页
	p.SetInt(offset, val) //往该缓存页中写入数据
	//该缓存页改动过活，如果他分发给其他人使用的话，就需要写入磁盘
	buff.SetModify(t.txNum, lsn) //记录下写入之前的信息
	return nil
}

//okToLog=true会生成记录，为false就不会生成对应的记录
func (t *Transaction) SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool) error {
	//调用同步管理器的x锁
	//t.xlock()
	buff := t.myBuffers.getBuf(blk)
	if buff == nil {
		//当前区块的数据并不存在
		return t.bufferNoExist(blk)
	}
	//把当前操作作为一个日志记录起来
	var lsn uint64
	var err error
	if okToLog {
		//生成记录
		lsn, err = t.recoverManager.SetString(buff, offset, val) //转发给recovermanager，由他在里面增加这个记录,毕竟是由他来恢复的
		if err != nil {
			return err
		}
	}
	p := buff.Contents()     //拿到他的缓存页
	p.SetString(offset, val) //往该缓存页中写入数据
	//该缓存页改动过活，如果他分发给其他人使用的话，就需要写入磁盘
	buff.SetModify(t.txNum, lsn) //记录下写入之前的信息
	return nil
}

//Size 获得当前文件占据了多少个block
func (t *Transaction) Size(filename string) uint64 {
	//调用S锁
	//dummy_blk:=fm.newBLockId(filename,uint64(endoffile))
	//t.concur_mgr.slock(dummmyblk)
	s, _ := t.fileManager.Size(filename)
	return s
}

//Append 给当前事务对应的文件增加一个区块
func (t *Transaction) Append(filename string) *fm.BlockId {
	//调用一个X锁
	blk, err := t.fileManager.Append(filename) //给当前文件增加一个区块
	if err != nil {
		return nil
	}
	return &blk

}
func (t *Transaction) BlockSize() uint64 {
	return t.fileManager.BlockSize()
}

func (t *Transaction) AvailableBuffer() uint64 {
	return uint64(t.bufferManager.Avaliable())
}
