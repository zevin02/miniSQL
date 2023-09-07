package transaction

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	bm "miniSQL/bufferManager"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
	"testing"
	"time"
)

func TestConcurrencyManager(t *testing.T) {
	/*
		出航舰3个线程，每个线程对应一个事务，这些事务都去读写相同的区块，判断区块读写时加锁的逻辑是否正确
	*/
	fmgr, err := fm.NewFileManager("/home/zevin/transaction_test", 400)
	lmgr, _ := lm.NewLogManager(fmgr, "logfile")
	bmgr := bm.NewBufferManager(fmgr, lmgr, 3) //开辟一个缓存管理器，内存池,供使用
	assert.Nil(t, err)
	go func() {
		txA := NewTransaction(fmgr, lmgr, bmgr) //打开一个事务
		//构建两个区块
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		//当前事务先把这两个区块占用
		txA.Pin(blk1)
		txA.Pin(blk2)
		fmt.Println("TX A,requst slock 1")
		txA.GetInt(blk1, 0) //从当前区块中读取数据
		fmt.Println("TX A,receive slock 1")
		time.Sleep(2 * time.Second) //把当前的线程启动起来
		fmt.Println("TX A request slock 2")
		txA.GetInt(blk2, 0) //从blk2中读取数据
		fmt.Println("TX A,receive slock 2")
		fmt.Println("TX A commit")
		txA.Commit() //把当前事务进行提交
	}()

	go func() {
		time.Sleep(1 * time.Second)             //先挂起1s,让3号线程启动起来
		txB := NewTransaction(fmgr, lmgr, bmgr) //打开一个事务
		//构建两个区块
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		//当前事务先把这两个区块占用
		txB.Pin(blk1)
		txB.Pin(blk2)
		fmt.Println("TX B,requst slock 2")
		//获得X锁
		txB.SetInt(blk2, 0, 0, false) //往blk2中写入数据
		fmt.Println("TX B,receive slock 2")
		time.Sleep(2 * time.Second) //停一下，让3号线程得到执行

		//获得S锁
		fmt.Println("TX B request slock 1")
		txB.GetInt(blk1, 0) //从blk1中读入数据
		fmt.Println("TX B,receive slock 1")
		txB.Commit() //把当前事务进行提交
	}()

	go func() {
		time.Sleep(2 * time.Second)
		txC := NewTransaction(fmgr, lmgr, bmgr) //打开一个事务
		//构建两个区块
		blk1 := fm.NewBlockId("testfile", 1)
		blk2 := fm.NewBlockId("testfile", 2)
		//当前事务先把这两个区块占用
		txC.Pin(blk1)
		txC.Pin(blk2)
		fmt.Println("TX C,requst slock 1")
		//获得X锁
		txC.SetInt(blk1, 0, 0, false) //往blk2中写入数据

		fmt.Println("TX A,receive slock 1")
		time.Sleep(1 * time.Second) //把当前的线程启动起来
		//获得S锁
		fmt.Println("TX c request slock 2")
		txC.GetInt(blk2, 0) //从blk1中读入数据
		fmt.Println("TX c,receive slock 2")
		txC.Commit() //把当前事务进行提交
	}()
	time.Sleep(20 * time.Second)
}
