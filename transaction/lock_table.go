package transaction

import (
	"errors"
	"fmt"
	"math/rand"
	fm "miniSQL/file_manager"
	"sync"
	"time"
)

const (
	//MAX_WAITING_TIME 假设多个线程要访问同一个区块，第一个线程对这个块进行写入操作，其他的几个要进行读操作，第一个要写操作的线程首先获得了这个互斥锁。其他的就获得共享锁
	//把要读取数据的线程挂起，
	MAX_WAITING_TIME = 3 //线程挂起的最长时间
)

//LockTable 同一个blockid，我们可以用两个不同的指针去指向这个同一个实例blockid，locktable是要针对同一个blockid进行处理的，所以使用指针做key就不能保证他的唯一性
type LockTable struct {
	lockMap    map[fm.BlockId]int64           //用来实现互斥锁和共享锁，互斥锁：-1,共享锁>0，如果有3个共享该区块，这个值就等于3
	notifyChan map[fm.BlockId]chan struct{}   //用于通知挂起的所有线程恢复执行的信号,针对每个blk
	notifyWg   map[fm.BlockId]*sync.WaitGroup //用于等待所有在某个blk中并发操作完成
	methodLock sync.Mutex                     //对当前locktable的所有操作都是需要加锁，放值locktable被同时读写
}

func NewLockTable() *LockTable {
	lockTable := &LockTable{
		lockMap:    make(map[fm.BlockId]int64),
		notifyChan: make(map[fm.BlockId]chan struct{}),
		notifyWg:   make(map[fm.BlockId]*sync.WaitGroup),
	}
	return lockTable
}

var lockTableInstance *LockTable //locktable的一个单例对象,全局只有一个的该对象
var lock = &sync.Mutex{}

//GetLockTableInstance 获得一个Locktable的单例对象
func GetLockTableInstance() *LockTable {
	lock.Lock()
	defer lock.Unlock()
	if lockTableInstance == nil {
		//当前实例还没有创建就构造一个对象出来
		lockTableInstance = NewLockTable()
	}
	return lockTableInstance
}

//initWaitingOnBlk 给某个区块进行初始化,如果已经初始化就不用管，没有初始化就需要进行初始化
func (l *LockTable) initWaitingOnBlk(blk fm.BlockId) {
	//如果某个区块没有初始化过的话，就需要进行初始化
	_, ok := l.notifyChan[blk]
	if !ok {
		l.notifyChan[blk] = make(chan struct{}) //给当前区块创建管道用来通信
	}

	//给当前区块创建一个通信集合
	_, ok = l.notifyWg[blk]
	if !ok {
		l.notifyWg[blk] = &sync.WaitGroup{}
	}

}

//waitGivenTimeOut 等待给定的时间，或者等待信号唤醒
//下面的两个方法都是在某个方法内部调用的，所以不需要加方法锁
func (l *LockTable) waitGivenTimeOut(blk fm.BlockId) {
	//当有线程去访问某个区块的时候，就会去调用该函数，来sleep一段时间，sleep完后再回来看看是否把锁释放了
	wg, ok := l.notifyWg[blk]
	if !ok {
		//对应的区块，还没有初始化该对象，则进行初始化该对象
		var newWg sync.WaitGroup
		l.notifyWg[blk] = &newWg
		wg = &newWg
	}
	wg.Add(1)             //将给定区块的计数+1,外面就能知道有多少个线程在访问该区块
	defer wg.Done()       //访问完之后，就将该区块的计数减1
	l.methodLock.Unlock() //因为当前已经被挂起了，所以不会访问里面的资源，所以要把这个锁进行释放,这样其他函数就可以进行该操作
	//两种信号会唤醒这个线程，一个就是当前已经等待超时了，另一个信号就是其他线程已经释放了这个区块的锁，就会通知所有等待该锁的对象进行唤醒
	select {
	case <-time.After(MAX_WAITING_TIME * time.Second):
		//等待超时信号到来
		fmt.Println("routine wake up for timeout")
	case <-l.notifyChan[blk]:
		//另外获得锁的线程已经操作完了,就会通知所有等待该锁的对象进行唤醒
		fmt.Println("toutine wake up for notify channel")
		//default说明这两个都没有，就在sleep
	}
	//走到外面来，说明要么被超时唤醒，要么被信号唤醒
	//需要所有线程重新进行加锁竞争
	l.methodLock.Lock()

}

//notifyAll 唤醒某个区块中所有等待的线程
func (l *LockTable) notifyAll(blk fm.BlockId) {
	s := fmt.Sprintf("close channel for blk :%v", blk)
	fmt.Println(s)
	channel, ok := l.notifyChan[blk] //获得信号
	if ok {
		//当前channel存在
		close(channel)            //发送信号,给某个线程
		delete(l.notifyChan, blk) //在里面删除这个，到时候调用的时候还会创建的
		mark := rand.Intn(10000)
		s = fmt.Sprintf("delete blk :%v and launch rotinue to create it ,mark :%d\n", blk, mark)
		fmt.Println(s)
		//等待所有线程返回
		go func(blkUnlock fm.BlockId, ranNum int) {
			//在匿名线程中执行wait，可以避免阻塞当前主线程,实现异步等待，
			s := fmt.Sprintf("wait group for blk: %v, with mark:%d\n", blkUnlock, ranNum)
			fmt.Print(s)

			l.notifyWg[blkUnlock].Wait() //我们需要等待所有线程都执行完之后，重新给该区块初始化一个新的管道，等待所有计数归零
			l.methodLock.Lock()
			l.notifyChan[blk] = make(chan struct{}) //重刑创建管道发送信号
			l.methodLock.Unlock()
			s = fmt.Sprintf("create notify channel for %v\n", blkUnlock)
			fmt.Print(s)
		}(blk, mark)
	} else {
		//当前并不存在
		s = fmt.Sprintf("channel for %v is already closed\n", blk)
		fmt.Print(s)
	}

}

//Slock (shared lock)共享锁
func (l *LockTable) Slock(blk fm.BlockId) error {
	//这个是外面可以直接调用的，所以需要进行加锁
	l.methodLock.Lock() //避免出现线程安全的问题
	defer l.methodLock.Unlock()
	l.initWaitingOnBlk(blk) //如果当前区块没有初始化的话，就需要进行初始化
	start := time.Now()
	//读读不互斥，读写互斥
	for l.hasXlock(blk) && !l.waitTooLong(start) {
		//如果当前区块已经有互斥锁，同时等待没有超时
		//在给定的时间中，如果有唤醒的话，就跳出循环了
		l.waitGivenTimeOut(blk) //挂起等待给定的时间
	}
	//出来之后，再次判断锁是否已经释放了
	if l.hasXlock(blk) {
		//互斥锁还没有释放，可能出现了死锁,当前操作就需要放弃
		fmt.Println("slock fail for xlock")
		return errors.New("Slock Exception:Xlock on given blk")
	}
	//Xlock已经被释放了
	//共享锁是可以同时作用在这个区块上的
	//val := l.getLockVal(blk)  //拿到锁对应的数值
	//l.lockMap[*blk] = val + 1 //共享锁+1
	l.incSlock(blk)
	return nil
}

//Xlock (exclusive Lock)排他锁
func (l *LockTable) Xlock(blk fm.BlockId) error {
	//这个是外面可以直接调用的，所以需要进行加锁
	l.methodLock.Lock() //避免出现线程安全的问题
	defer l.methodLock.Unlock()
	l.initWaitingOnBlk(blk) //同样，当前在使用某个区块，如果当前区块的管道没有创建的话 ，就进行创建，如果已经存在的话，就不需要创建
	start := time.Now()
	//读读不互斥，读写互斥,写写互斥
	for l.hashOtherSlock(blk) && !l.waitTooLong(start) {
		//如果当前区块已经有共享锁，同时等待没有超时
		l.waitGivenTimeOut(blk) //挂起等待给定的时间,等待锁的释放
	}
	if l.hashOtherSlock(blk) {
		//共享锁还没有释放，可能出现了死锁,当前操作就需要放弃
		fmt.Println("xlock fail for slock")
		return errors.New("Xlock Exception:Slock on given blk")
	}
	l.lockMap[blk] = -1 //-1表示互斥锁
	return nil
}

//IncSlockWithLock 增加Slock锁的引用计数
func (l *LockTable) IncSlockWithLock(blk fm.BlockId) {
	l.methodLock.Lock()
	defer l.methodLock.Unlock()
	l.incSlock(blk)
}

func (l *LockTable) incSlock(blk fm.BlockId) {
	val := l.getLockVal(blk) //拿到锁对应的数值
	l.lockMap[blk] = val + 1 //共享锁+1
}

//UnLock 将某个区块的锁全部释放
func (l *LockTable) UnLock(blk fm.BlockId) {
	l.methodLock.Lock() //避免出现线程安全的问题
	defer l.methodLock.Unlock()
	//获得该锁对应的数值
	val := l.getLockVal(blk)
	if val > 0 {
		//共享锁
		l.lockMap[blk] = val - 1
	} else {
		//互斥锁
		//l.lockMap[blk] = 0 //设置他成为0,都表示他没有锁
		delete(l.lockMap, blk) //删除这个元素
		l.notifyAll(blk)       //互斥锁释放了，给所有等待中的线程发消息
	}
}

//hasXlock 判断是否有Xlock加在某个区块上
func (l *LockTable) hasXlock(blk fm.BlockId) bool {
	return l.getLockVal(blk) < 0
}

//hashOtherSlock 判断是否有其他的事务也使用该读锁,如果=1说明是我们当前事务自己使用的
func (l *LockTable) hashOtherSlock(blk fm.BlockId) bool {
	return l.getLockVal(blk) > 1 //如果=1说明是我们自己得到的
}

//waitTooLong 判断是否等待超时了
func (l *LockTable) waitTooLong(start time.Time) bool {
	//我们看看是否有超时
	elapsed := time.Since(start).Seconds() //看看等待的时间
	if elapsed >= MAX_WAITING_TIME {
		//超过了给定时间，就返回true
		return true
	}
	//没有等待超时
	return false
}

//getLockVal 获得某个区块的数值
func (l *LockTable) getLockVal(blk fm.BlockId) int64 {
	val, ok := l.lockMap[blk]
	if !ok {
		l.lockMap[blk] = 0
		//当前不存在
		return 0
	} else {
		return val
	}
}
