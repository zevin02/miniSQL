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

//Pin 将给定磁盘文件的区块数据分配给缓存页面,相当于内存分配
func (b *BufferManager) Pin(blk *fm.BlockId) (*Buffer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	start := time.Now()   //获得当前的时间
	buff := b.tryPin(blk) //尝试分配缓存页面
	for buff == nil && b.waitingTooLong(start) == false {
		//进来的话，就说明没有等待过长的时间
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
		//如果当前buffer的引用计数=0,就可以继续进行分配给其他人使用了
		b.numAvailable++ //可用的buffer数+1
		//NoTifyALL 唤醒所有在尝试pin页面的组建，唤醒来调用新的page,并发管理器的内容
	}
}

//waitingTooLong 查看是否等待了过长的时间
func (b *BufferManager) waitingTooLong(start time.Time) bool {
	elapesed := time.Since(start).Seconds() //查看至今过了多长时间
	if elapesed >= MAX_TIME {
		return true
	}
	return false
}

//tryPin 尝试去获得一块buffer的数据
func (b *BufferManager) tryPin(blk *fm.BlockId) *Buffer {
	buff := b.findExistingBuffer(blk) //在buffer管理器中检查给定区块是否已经被读取到缓冲区中了
	if buff == nil {
		//当前区块没有被读取到，那么就需要去将当前区块从磁盘中读取上来
		buff = b.chooseUnpinBuffer() //查看是否还有可用的缓存页面，有的话， 就的可以得到当前的buffer块，同时需要将给定磁盘数据写入缓存中,
		if buff == nil {
			//没有找到可用的缓存页面
			return nil
		}
		//分配完缓存页面之后，将blk指向区块的数据读取到缓存中进行管理,如果当前区块之前有缓存数据的话，就需要将该区块缓存的数据给刷新到磁盘中
		buff.Assign2Block(blk)
	}
	if buff.IsPinned() == false {
		//如果当前的buffer=0,说明还没有人使用，同时申请成功了
		b.numAvailable--
	}
	buff.Pin() //增加引用计数
	return buff
}

//findExistingBuffer 检查需要的blockid是否已经存在了
func (b *BufferManager) findExistingBuffer(blk *fm.BlockId) *Buffer {
	for _, buffer := range b.bufferPool {
		block := buffer.Block() //获得当前block的信息
		if block != nil && block.Equal(blk) {
			//如果当前block不为空，同时这个block和要查找的block一致，说明这个block已经存在了，就不需要再创建新block
			return buffer
		}
	}
	//当前blk没有被找到
	return nil
}

//chooseUnpinBuffer 在bufferpool中查找可用的buffer,引用计数=0的页面
func (b *BufferManager) chooseUnpinBuffer() *Buffer {
	for _, buffer := range b.bufferPool {
		if !buffer.IsPinned() {
			//如果发现了某个还没有被使用的buffer，就可以被返回供使用
			return buffer
		}
	}
	//说明全部的buffer都被使用了
	return nil
}
