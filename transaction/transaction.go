package transaction

import (
	"errors"
	"fmt"
	bm "miniSQL/buffer_manager"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
	"sync"
)

var txNumMu sync.Mutex     //事务的锁
var nextTxNumId = int32(0) //事务的序列号,全局的事务，每次创建一个事务的时候，这个事务序列号就会自增

//获得下一个事务的id
func nextTxNum() int32 {
	txNumMu.Lock()
	defer txNumMu.Unlock()
	nextTxNumId++
	return nextTxNumId
}

//Transaction 管理某个事务
type Transaction struct {
	myBuffers      *BufferList         //管理当前事务被pin的buffer对象
	logManager     *lm.LogManager      //日志管理,所有的日志都由该日志管理器的日志记录
	fileManager    *fm.FileManager     //文件管理
	recoverManager *RecoveryManager    //恢复管理器,用于事务恢复或者回滚
	txNum          int32               //当前的事务序列号
	bufferManager  *bm.BufferManager   //缓存管理器,管理当前事务使用缓存
	concurrentMgr  *ConcurrencyManager //管理并发请求
}

//NewTransaction 构造一个事务对象，传入的是文件管理器，缓存管理器，日志管理器
func NewTransaction(fileManager *fm.FileManager, logManager *lm.LogManager, bufferManager *bm.BufferManager) *Transaction {
	tx := &Transaction{
		fileManager:   fileManager,
		logManager:    logManager,
		bufferManager: bufferManager,
		myBuffers:     NewBufferList(bufferManager), //构造一个bufferList对象，传入对应的缓存管理器即可
		txNum:         nextTxNum(),
	}
	//创建同步管理器，管理锁
	tx.concurrentMgr = NewConcurrencyManager()

	//创建恢复管理器，
	//当前事务创建，就相当于一个事务启动了,START X
	tx.recoverManager = NewRecoverManager(tx, tx.txNum, logManager, bufferManager)
	return tx
}

//lock point（锁点）：当前事务获得最后一把锁的时间节点,使用两阶段锁可以保证实现事务之间执行的串型化，但是两阶段锁不会确保不会出现死锁，也可能会导致迭代rollback

//Commit 将当前的事务进行提交,并把当前数据刷盘
func (t *Transaction) Commit() {
	//在commit之前把锁给释放掉，收缩阶段(事务必须锁，事务不能在获得锁)的，严格两阶段锁
	t.concurrentMgr.Realse()
	err := t.recoverManager.Commit()
	if err != nil {
		return
	}
	r := fmt.Sprintf("transaction %d commited", t.txNum)
	fmt.Println(r)
	//执行commit之后，当前事务就全部完成了，所有的数据都会写入到磁盘中去，将当前用于存储当前缓存页全部进行解锁，解引用
	t.myBuffers.UnpinAll()
}

//RollBack 执行一个回滚操作,好像当前的所有事务没有发生一样,丢弃当前事务，恢复到事务发生之前的状态
func (t *Transaction) RollBack() error {
	err := t.recoverManager.RollBack()
	if err != nil {
		return err
	}
	t.concurrentMgr.Realse() //回滚的时候也需要释放锁
	r := fmt.Sprintf("transaction %d roll back", t.txNum)
	fmt.Println(r)

	//同样也是要释放同步管理器
	t.myBuffers.UnpinAll() //释放当前事务的所有缓存页
	return nil
}

//Recover 系统启动的时候，会在所有事务执行前，运行该函数
//系统启动的时候发现上一次的事务在执行到一半的时候，发生崩溃或者断电了，数据写到一半，启动之后，就要将写到一半的数据给抹掉，恢复到写入之前的状态
func (t *Transaction) Recover() error {
	t.bufferManager.FlushAll(t.txNum) //把当前事务还没有处理完的数据全部写入到磁盘
	err := t.recoverManager.Recover()
	if err != nil {
		return err
	}
	return nil
}

func (t *Transaction) Pin(blk *fm.BlockId) error {
	err := t.myBuffers.Pin(blk)
	if err != nil {
		return err
	} //调用pin进行管理,
	return nil
}

func (t *Transaction) Unpin(blk *fm.BlockId) error {
	t.myBuffers.Unpin(blk) //调用pin进行管理
	return nil
}

//bufferNoExist 缓存不存在
func (t *Transaction) bufferNoExist(blk *fm.BlockId) error {
	errS := fmt.Sprintf("No Buffer found  for given blk %d with filename: %s\n", blk.Number(), blk.FileName())
	err := errors.New(errS)
	return err
}

//GetInt 在事务中读取一个数据
//读取数据,直接让强转(int64)
func (t *Transaction) GetInt(blk *fm.BlockId, offset uint64) (int64, error) {
	//读取数据的时候，调用同步管理器加s类型的锁,加锁的数据范围要尽可能小
	err := t.concurrentMgr.SLock(blk) //加上共享锁
	if err != nil {
		return -1, err
	}
	//mybuf中没有数据
	buff := t.myBuffers.getBuf(blk)
	if buff == nil {
		//当前区块的数据并不存在
		return -1, t.bufferNoExist(blk)
	}
	return buff.Contents().GetInt(offset), nil //在指定的buff中获得相应的数据

}

//GetString 从事务中的某个区块的文件中读取数据
func (t *Transaction) GetString(blk *fm.BlockId, offset uint64) (string, error) {
	//调用同步管理器加s锁
	err := t.concurrentMgr.SLock(blk)
	if err != nil {
		return "", err
	} //加上共享锁
	buff := t.myBuffers.getBuf(blk)
	if buff == nil {
		//当前区块的数据并不存在
		return "", t.bufferNoExist(blk)
	}
	return buff.Contents().GetString(offset), nil //从相应的地方获得指定的数据
}

//SetInt okToLog=true会生成记录，为false就不会生成对应的记录
func (t *Transaction) SetInt(blk *fm.BlockId, offset uint64, val int64, okToLog bool) error {
	//使用并发管理器加上排他锁
	err := t.concurrentMgr.XLock(blk)
	//如果当前出错的话，就有可能出现死锁，就需要进行回滚，把所有当前的锁给解开
	if err != nil {
		return err
	}
	buff := t.myBuffers.getBuf(blk)
	if buff == nil {
		//当前区块的数据并不存在
		return t.bufferNoExist(blk)
	}
	//把当前操作作为一个日志记录起来
	var lsn uint64
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
	buff.SetModify(t.txNum, lsn) //更新当前buffer对应的事务以及日志，标记该缓存已经被修改了
	//添加这个到脏页
	t.bufferManager.AddToDirty(buff)

	return nil
}

//SetString okToLog=true会生成记录，为false就不会生成对应的记录
//undo的时候也会调用这个Setstring操作，把数据写入到事务中
func (t *Transaction) SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool) error {
	//调用同步管理器的x锁
	//在写入的时候使用并发管理器加上排他锁
	err := t.concurrentMgr.XLock(blk)
	if err != nil {
		return err
	}
	buff := t.myBuffers.getBuf(blk)
	if buff == nil {
		//当前区块的数据并不存在
		return t.bufferNoExist(blk)
	}
	//把当前操作作为一个日志记录起来
	var lsn uint64
	if okToLog {
		//生成记录
		lsn, err = t.recoverManager.SetString(buff, offset, val) //转发给recoverManager，由他在里面增加这个记录,毕竟是由他来恢复的
		if err != nil {
			return err
		}
	}
	p := buff.Contents()     //拿到他的缓存页
	p.SetString(offset, val) //往该缓存页中写入数据
	//该缓存页改动过活，如果他分发给其他人使用的话，就需要写入磁盘
	buff.SetModify(t.txNum, lsn) //记录下写入之前的信息
	t.bufferManager.AddToDirty(buff)

	return nil
}

//Size 获得当前文件占据了多少个block
//Size和Append操作是互斥的操作，读写互斥
func (t *Transaction) Size(filename string) (uint64, error) {
	//读取当前文件的大小的时候，也是需要调用S锁，共享
	dummyBlk := fm.NewBlockId(filename, uint64(END_OF_FILE)) //构造一个虚拟的BlockId来进行管理
	err := t.concurrentMgr.SLock(dummyBlk)
	//给这个文件区块加共享锁
	if err != nil {
		return 0, err
	}

	s, _ := t.fileManager.Size(filename)
	return s, nil
}

//Append 给当前事务对应的文件增加一个区块
func (t *Transaction) Append(filename string) (*fm.BlockId, error) {
	//调用一个X锁
	dummyBlk := fm.NewBlockId(filename, END_OF_FILE) //构造一个虚拟的BlockId来进行管理
	err := t.concurrentMgr.XLock(dummyBlk)
	if err != nil {
		return nil, err
	} //给这个文件区块加共享锁

	blk, err := t.fileManager.Append(filename) //给当前文件增加一个区块
	if err != nil {
		return nil, err
	}
	return &blk, nil

}

//BlockSize 获得缓存块大小
func (t *Transaction) BlockSize() uint64 {
	return t.fileManager.BlockSize()
}

//AvailableBuffer 获得当前缓存管理器还可用的缓存块个数
func (t *Transaction) AvailableBuffer() uint64 {
	return uint64(t.bufferManager.Avaliable())
}
