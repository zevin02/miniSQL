package transaction

import fm "miniSQL/file_manager"

//ConcurrencyManager 并发控制器，并发控制器可以有多个实例，但是只可共用一个Locktable,说明他是一个单例对象
type ConcurrencyManager struct {
	lockTable *LockTable
	lockMap   map[fm.BlockId]string //当前的blk对象使用的lock类型
}

func NewConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{
		lockMap:   make(map[fm.BlockId]string),
		lockTable: GetLockTableInstance(), //获得；locktable的单例对象
	}
}

//SLock 上共享锁
func (c *ConcurrencyManager) SLock(blk *fm.BlockId) error {
	_, ok := c.lockMap[*blk] //查看
	if !ok {
		//当前的blk还没有被创建出来
		err := c.lockTable.Slock(blk) //获得该锁
		if err != nil {
			return err
		}
		c.lockMap[*blk] = "S" //当前该区块获得的是S锁
	}
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
