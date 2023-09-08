package transaction

import (
	"errors"
	fm "miniSQL/file_manager"
)

//ConcurrencyManager 并发控制器，并发控制器可以有多个实例，但是只可共用一个Locktable,说明他是一个单例对象
type ConcurrencyManager struct {
	lockTable *LockTable
	lockMap   map[fm.BlockId]string //某一个事务对应使用的文件块以及他的类型
}

func NewConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{
		lockMap:   make(map[fm.BlockId]string),
		lockTable: GetLockTableInstance(), //获得locktable的单例对象
	}
}

//SLock 上共享锁
func (c *ConcurrencyManager) SLock(blk *fm.BlockId) error {
	val, ok := c.lockMap[*blk] //查看
	if c.hashXLock(blk) {
		//如果blk当前已经被占用了,切实X锁
		return errors.New("block is locked by another transaction")
	}
	if !ok {
		//当前的blk还没有被任何锁占用，我们直接占用即可
		err := c.lockTable.Slock(blk) //获得该锁，如果里面有X锁的话，也会停止
		if err != nil {
			return err
		}
		c.lockMap[*blk] = "S" //当前该区块获得的是S锁
	} else if val == "S" {
		//如果当前是S锁增加着把锁的引用计数
		c.lockTable.IncSlockWithLock(blk)
	}
	//当前的blk已经被占用了
	return nil
}

//XLock 上排他锁
func (c *ConcurrencyManager) XLock(blk *fm.BlockId) error {
	//如果当前有X锁存在的话，就无法访问
	if !c.hashXLock(blk) {
		//当前没有排他锁被占用
		err := c.lockTable.Xlock(blk)
		if err != nil {
			return err
		}
		c.lockMap[*blk] = "X" //当前该区块获得的是S锁
	}
	return nil
}

//hashXLock 判断当前是否有X锁
func (c *ConcurrencyManager) hashXLock(blk *fm.BlockId) bool {
	lockType, ok := c.lockMap[*blk]
	return ok && lockType == "X"
}

//Realse 对所有区块的锁，全部释放，两阶段，释放阶段
func (c *ConcurrencyManager) Realse() {
	//遍历所有的blk块
	for blk, _ := range c.lockMap {
		c.lockTable.UnLock(&blk)
	}
}
