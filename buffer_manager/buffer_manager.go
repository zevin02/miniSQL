package buffer_manager

import (
	"container/list"
	"errors"
	"miniSQL/container"
	fm "miniSQL/file_manager"
	lm "miniSQL/log_manager"
	"sync"
	"time"
)

/*
	数据库系统要处理高吞吐量的数据，数据不能全部存储在内存中，但是也不能频繁访问磁盘，所以我们需要权衡数据在内存和磁盘中的存储
	设计一个缓存管理器预先分配一定的内存页，形成一个内存池，当其他组建需要读写数据的时候，先通过缓存管理器获得内存页，

	缓存管理器并不会在乎数据的一致性，多个客户端在使用同一个内存页面的时候，可以随意的对数据进行读写，(这个需要我们后续的并发管理器来进行处理)

	当前页面写入磁盘：情况一：当前页面读取其他区块的数据
				   情况二：相应的写接口被调用
			       情况3：当前事务调用commit接口，结束事务的时候，就会脏页刷新到磁盘中
				   情况4：当前buff被添加到脏队列的时候，当前的buff在被unpin的时候，就会检查脏队列，判断是否可以刷盘

	磁盘预读：磁盘的提前加载符合局部性原理，可以减少磁盘的IO，我们能把需要的页提前加入到缓冲池中，避免未来的磁盘IO操作
	我们需要让预读失败的页，停留再LRU中的时间尽可能短，

	我们不能遍历这一片连续的内存空间来找到空闲的缓存页，这个效率非常低，所以为了能够快速找到空闲的缓存页表，可以使用链表结构，将空闲的缓存页作为链表的节点（free链表）
	TODO 在minisql实例启动的时候，开启4个后台线程，将脏页链表放成全局访问，当脏页数量达到缓冲区数量的1/10的时候，就会进行将脏页进行刷盘,延迟刷盘的时间

*/

//如果有3个buffer，4个请求，那么前3个请求得到了，第4个请求就需要进行等待，最多等待3s
const (
	MAX_TIME = 3 //分配页面的时候最多等待的时间
)

//BufferManager 缓存管理器
type BufferManager struct {
	//TODO 可以将缓存池分成多个，降低锁的粒度

	bufferPool   []*Buffer //缓存池
	numAvailable uint32    //缓存池中有多少个页面可以使用
	mu           sync.RWMutex
	lruCache     *container.LRUCache
	freelist     *list.List         //管理空闲页
	dirtylist    map[string]*Buffer //管理脏页,todo 后期放在全局
}

//NewBufferManager 开辟一个缓存管理器对象
func NewBufferManager(fileManager *fm.FileManager, logManager *lm.LogManager, numBuffer uint32) *BufferManager {
	bufferManager := &BufferManager{
		numAvailable: numBuffer, //有多少个页面可以使用
		lruCache:     container.NewLRUCache(int(numBuffer), time.Second, 0.25),
		freelist:     list.New(),
		dirtylist:    make(map[string]*Buffer),
	}

	//根据缓存池中的数量来分配需要buffer,开辟内存池
	for i := uint32(0); i < numBuffer; i++ {
		buffer := NewBuffer(fileManager, logManager)                        //申请一个buffer出来
		bufferManager.bufferPool = append(bufferManager.bufferPool, buffer) //将申请的buffer添加到池中进行管理
		bufferManager.freelist.PushFront(buffer)                            //把当前的节点放到freelist中进行管理
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
	//在缓冲池中查找脏页符合txnum

	var toDelete []*Buffer //把脏页集合起来
	//从当前的脏页池中把相关的筛选出来
	for _, buffer := range b.dirtylist {
		if buffer.ModifyingTx() == txnum {
			buffer.Flush() //把当前的缓冲区给刷新到磁盘中
			//再把这个节点删除掉
			toDelete = append(toDelete, buffer)
		}
	}
	//在脏页缓存区在把这些给删除掉
	for _, buffer := range toDelete {
		delete(b.dirtylist, buffer.blk.HashCode())
	}

}

//Pin 将给定磁盘文件的区块数据分配给缓存页面,相当于内存分配，new
/*
	情况一：要读取的数据已经被缓存在某个页面中了，这样缓存器就设置这个页面pin，增加引用计数
	情况二：数据还没有被读取进缓存中，缓存中还有空闲的页面可以使用，就可以直接获得该页面
	情况三：数据还没有读入内存中，也没有空闲页面使用，就需要等待

	使用超时机制来解决死锁问题，如果长时间没有可以用的页面，就需要返回错误

*/

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
	//读取pin成功之后，再尝试看看能不能提前得到
	nextBlk := fm.NewBlockId(blk.FileName(), blk.Number()+1)
	//通过后台进行读取
	if !b.lruCache.Contain(nextBlk.HashCode()) {
		//检查前一个page是否存在，如果当前不存在，就需要进行异步的读取,同时把检查未被pin过的缓存页表
		//异步的话，在读写的时候，可能会发生冲突
		b.asyncPreRead(nextBlk)
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
		b.lruCache.Remove(buffer.blk.HashCode())

		if buffer.txnum != -1 {
			//如果当前的buffer不在脏页区中，就不需要刷新
			if _, ok := b.dirtylist[buffer.blk.HashCode()]; ok {
				buffer.Flush()
				delete(b.dirtylist, buffer.blk.HashCode())
			}
			//这个刷盘完了，就把这个buffer给删除掉

		}
		b.freelist.PushFront(buffer) //添加到空闲列表中
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
	//从LRU缓存中获得缓存页面
	if cacheItem, ok := b.lruCache.Get(blk.HashCode()); ok {
		//得到了缓存页
		buffer := cacheItem.(*Buffer)

		buffer.Pin() //增加引用计数，获得到之后，就需要增加引用计数，把当前page占用了，TODO 检测如果是同一个线程多次pin同一个区块的话，引用计数会不会增加
		return buffer
	}
	//LRU缓存中不存在，尝试从buffer pool中获取

	//当前区块没有被读取到，那么就需要去将当前区块从磁盘中读取上来,在这里如果缓存已经满了，就需要执行页面替换
	buff := b.chooseUnpinBuffer() //查看是否还有可用的缓存页面，有的话， 就的可以得到当前的buffer块，同时需要将给定磁盘数据写入缓存中,
	if buff == nil {
		//没有找到可用的缓存页面
		return nil
	}
	//分配完缓存页面之后，将blk指向区块的数据读取到缓存中进行管理,如果当前区块之前有缓存数据的话，就需要将该区块缓存的数据给刷新到磁盘中
	//TODO 可以在读取缓存页的时候，进行提前的预读取

	buff.Assign2Block(blk)               //先落盘
	b.lruCache.Set(blk.HashCode(), buff) //将这个缓存页表加入到缓存中

	//预先读取相邻的page
	if buff.IsPinned() == false {
		//如果当前的buffer=0,说明还没有人使用，同时申请成功了
		b.numAvailable--
	}
	buff.Pin() //增加引用计数
	return buff
}

//chooseUnpinBuffer 在bufferpool中查找可用的buffer,引用计数=0的页面
func (b *BufferManager) chooseUnpinBuffer() *Buffer {
	//LRU置换算法
	if b.freelist.Len() != 0 {
		elem := b.freelist.Front() //取出头节点
		buffer := elem.Value.(*Buffer)
		b.freelist.Remove(elem) //把这个给删除掉
		return buffer
	}
	//说明全部的buffer都被使用了
	return nil
}

//asyncPreRead 异步的进行磁盘数据的预读取
//先检查当前区块是否存在，再将数据读取上来，先不进行pin，添加到LRU中，等待LRU读取的时候，再占用，如果这个块一直没有被使用，因为没有被pin，所以后续别的块也可以使用
//同时由于他一直没有被使用，一直在冷链表中，所以在缓存淘汰的时候，也会优先被淘汰
func (b *BufferManager) asyncPreRead(blk *fm.BlockId) {
	//先查看是否能获得一个缓存块
	buff := b.chooseUnpinBuffer()
	if buff == nil {
		return
	}
	//当前获得了一个缓存块
	size, err := buff.fileManager.Size(blk.FileName()) //获得当前的size大小
	if err != nil {
		return
	}
	if blk.Number() >= size || blk.Number() < 0 {
		//当前的blk不合法，直接就可以返回
		b.freelist.PushFront(buff)
		return
	}
	//当前的blk合法
	buff.Assign2Block(blk) //把这个缓存块之前的值先把他落盘

	b.lruCache.Set(blk.HashCode(), buff) //添加到LRU缓存中
	//不需要把这个blk集逆性pin，在lru缓存读取到这个blk的时候，才会进行pin
	return

}

//AddToDirty 把当前的buff添加到脏页列表中
func (b *BufferManager) AddToDirty(buff *Buffer) {
	_, ok := b.dirtylist[buff.blk.HashCode()]
	if ok {
		//如果当前的脏页已经存在了，就不需要添加了
	} else {
		//将当前的缓存页添加到脏页列表进行管理
		b.dirtylist[buff.blk.HashCode()] = buff
	}
}
