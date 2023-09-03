package bufferManager

import (
	"errors"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
	"sync"
	"time"
)

//如果有3个buffer，4个请求，那么前3个请求得到了，第4个请求就需要进行等待，最多等待3s
const (
	MAX_TIME = 3 //分配页面的时候最多等待的时间
)

type BufferManager struct {
	bufferPool   []*Buffer //缓存池
	numAvailable uint32    //缓存池中有多少个页面可以使用
	mu           sync.RWMutex
}

func NewBufferManager(fileManager *fm.FileManager, logManager *lm.LogManager, numBuffer uint32) *BufferManager {
	bufferManager := &BufferManager{
		numAvailable: numBuffer, //有多少个页面可以使用
	}

	//根据缓存池中的数量来分配需要buffer
	for i := uint32(0); i < numBuffer; i++ {
		buffer := NewBuffer(fileManager, logManager)                        //申请一个buffer出来
		bufferManager.bufferPool = append(bufferManager.bufferPool, buffer) //将申请的buffer添加到池中进行管理
	}
	return bufferManager
}

//Avaliable 返回当前可以用的缓存页面的数量
func (b *BufferManager) Avaliable() uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.numAvailable
}

//FlushAll 将所有txnum事务的缓存数据都刷新到磁盘中
func (b *BufferManager) FlushAll(txnum int32) {
	//将所有txnum相同事务中的数据全都刷新到磁盘中
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, buffer := range b.bufferPool {
		if buffer.ModifyingTx() == txnum {
			buffer.Flush() //如果当前buffer中正在修改的数据就是txbum，那么这个buffer就可以被刷新到磁盘中了
		}
	}
}

//Pin 将给定磁盘文件的区块数据分配给缓存页面
func (b *BufferManager) Pin(blk *fm.BlockId) (*Buffer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	start := time.Now()   //获得当前的时间
	buff := b.tryPin(blk) //尝试分配缓存页面
	for buff == nil && b.waitingTooLong(start) == false {
		//如果没有分配到缓存页面，那么就等待一段时间看看有没有可以使用的缓存页面
		time.Sleep(MAX_TIME * time.Second) //等待一段时间
		//再次尝试获得数据
		buff = b.tryPin(blk)
		if buff == nil {
			//如果还是没有分配到页面，就结束
			return nil, errors.New("No buffer available,careful for dead lock")
		}
	}
	return buff, nil
}

func (b *BufferManager) Unpin(buffer *Buffer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if buffer == nil {
		return
	}
	buffer.Unpin() //把他的引用计数减1
	//如果没有再有组建引用的话，就需要将当前的缓存写回到磁盘中
	if !buffer.IsPinned() {
		//当前缓存页面已经没有再被使用了，
		b.numAvailable++ //可用的buffer数+1
		//NoTifyALL 唤醒所有在尝试pin页面的组建，唤醒来调用新的page,并发管理器的内容

	}
}

func (b *BufferManager) waitingTooLong(start time.Time) {

}