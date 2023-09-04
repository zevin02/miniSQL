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
	myBuffers      *BufferList        //管理当前的缓存
	logManager     *lm.LogManager     //日志管理
	fileManager    *fm.FileManager    //文件管理
	recoverManager *rm.RecoverManager //恢复管理器
	txNum          int32              //当前的事务序列号
	bufferManager  *bm.BufferManager  //缓存管理器

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
func (t *Transaction) GetInt(blk *fm.BlockId, offset uint64) int64 {
	//读取数据的时候，调用同步管理器加s类型的锁

}
