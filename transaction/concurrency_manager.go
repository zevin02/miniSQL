package transaction

import (
	fm "miniSQL/file_manager"
)

//ConcurrencyManager 并发控制器，并发控制器可以有多个实例，但是只可共用一个Locktable,说明他是一个单例对象
type ConcurrencyManager struct {
	lockTable *LockTable
	lockMap   map[fm.BlockId]string //某一个事务对应使用的文件块以及他的类型，这个是事务特有的
}

//NewConcurrencyManager 构造一个并发管理器对象
func NewConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{
		lockMap:   make(map[fm.BlockId]string),
		lockTable: GetLockTableInstance(), //获得locktable的单例对象
	}
}

//SLock 上共享锁,由于SLock当前的slock只是针对当前的对象来说,如果当前对象已经, todo 将这里的指针全部改成对象
func (c *ConcurrencyManager) SLock(blk fm.BlockId) error {
	_, ok := c.lockMap[blk] //在当前事务对应的锁中查看是否使用该区块
	if !ok {
		//当前事务对象并没有占用该区块,所以可以去获得
		err := c.lockTable.Slock(blk) //获得该锁，如果里面有X锁的话，也会停止
		if err != nil {
			return err
		}
		c.lockMap[blk] = "S" //当前该区块获得的是S锁
	}
	//当前事务对blk区块已经占用了锁，读和写操作都可以
	return nil

}

//XLock 上排他锁
func (c *ConcurrencyManager) XLock(blk fm.BlockId) error {
	//如果当前有X锁存在的话，就无法访问
	if !c.hashXLock(blk) {
		c.SLock(blk) //保证了读锁的值=1
		/*
			在获得写锁之前先获得读锁，应为同一个线程可以在获得某个对象的读锁的情况下，再获得该对象的写锁
			获得读锁的时候，读锁的计数会+1,如果读锁的计数>1说明其他线程对同一个分区加了读锁，此时获得写锁就是失败的
			如果读锁的计数=1,说明读锁是我们现在得到的，也就是同一个线程在得到读锁的同时，也能获得他的写锁
		*/
		//当前没有排他锁被占用
		err := c.lockTable.Xlock(blk)
		if err != nil {
			return err
		}
		c.lockMap[blk] = "X" //当前该区块获得的是S锁
	} //如果当前对象已经获得了该区块的X锁的话，就可以直接使用，如果没有X锁的话，就需要去申请得到
	return nil
}

//hashXLock 判断当前是否有X锁
func (c *ConcurrencyManager) hashXLock(blk fm.BlockId) bool {
	lockType, ok := c.lockMap[blk]
	return ok && lockType == "X"
}

//Release 对所有区块的锁，全部释放，两阶段，释放阶段
func (c *ConcurrencyManager) Release() {
	//遍历所有的blk块
	for blk, _ := range c.lockMap {
		c.lockTable.UnLock(blk)
		//把当前事务的lockmap释放掉,
		delete(c.lockMap, blk)
	}
}
