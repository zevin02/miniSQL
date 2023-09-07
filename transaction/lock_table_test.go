package transaction

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	fm "miniSQL/file_manager"
	"sync"
	"testing"
	"time"
)

/*

 */

func TestLockTableWithSLockAfterXLockRelease(t *testing.T) {
	/*
		    启动4个线程，第一个线程对区块1加上互斥锁，剩下3个线程对区块1加共享锁
		    剩下的3个线程因为区块1,加了互斥锁而无法得到，所以就会进行等待，挂起，等待锁释放
			从而得到共享锁，成功读取区块1的数据
	*/
	blk := fm.NewBlockId("testfile", 1)
	var err_array []error         //收集各个线程中的错误消息
	var err_array_lock sync.Mutex //对该错误队列加锁
	lockTable := NewLockTable()
	lockTable.Xlock(blk)  //使用互斥锁，将这个区块占用
	var wg sync.WaitGroup //等待所有线程完成

	for i := 0; i < 3; i++ {
		go func(i int) {
			//这3个线程使用共享锁访问，由于前面Xlock的存在，会挂起一段时间
			fmt.Printf("routine %d start \n", i)
			wg.Add(1) //增加对线程的管理
			defer wg.Done()
			err_array_lock.Lock()
			defer err_array_lock.Unlock()
			err := lockTable.Slock(blk) //使用互斥锁
			if err == nil {
				fmt.Printf("routine %d access slock ok\n", i)
			} else {
				fmt.Printf("routine %d access slock fail\n", i)
			}
			err_array = append(err_array, err)
		}(i)
	}
	time.Sleep(1 * time.Second) //让3个线程都跑起来
	lockTable.UnLock(blk)       //将blk区块解锁（可能是解锁共享锁，也可能是解锁互斥锁）
	//这里解锁，就会把所有的线程给唤醒
	start := time.Now()
	wg.Wait()                              //等待3个线程完成
	elapsed := time.Since(start).Seconds() //这个通常会小于最长等待的时间
	fmt.Println("elcasep time :", elapsed)
	assert.Equal(t, elapsed < MAX_WAITING_TIME, true)
	assert.Equal(t, len(err_array), 3)
	for i := 0; i < 3; i++ {
		fmt.Println(err_array[i])
		assert.Nil(t, err_array[i])
	}
	assert.Equal(t, int64(3), lockTable.lockMap[blk])
}
