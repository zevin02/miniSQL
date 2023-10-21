package container

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

//预读：当从磁盘上读取某个数据页的时候，会将这个数据页相邻的其他数据页也读取上来
//1.触发：顺序访问磁盘上一个区的多个数据页，数量超过了一定的值，表示对下一个区域也感兴趣，就会将下一个区加载到buffer pool中
//2.

//冷热分离，将LRU链表分成两部分，一部分用来存放冷数据，刚从磁盘中读取上来的数据，一部分存放热点数据，冷数据占比37%
//刚读取上来的数据放在冷数据的头部，如果这些缓存页在1s后再访问，就移动到热数据的头部，如果1s内再访问就不移动，就在冷数据区中（可能是一个偶然的访问，不足以说明他是热数据）
type LOCATION byte

const (
	HOT LOCATION = iota
	COLD
)

type CacheItem struct {
	key        string
	value      interface{}
	insertTime time.Time
	loocation  LOCATION
}

//LRUCache 冷数据+热数据的总长度应该是capcaity大小
type LRUCache struct {
	capacity         int                      // LRU 缓冲区的容量
	cache            map[string]*list.Element // map 的 string 对应一个双向链表的节点元素，快速访问节点元素
	hotDataList      *list.List               // 存储节点的双向链表
	coldDataList     *list.List               // 存放冷数据的
	mu               sync.Mutex               // 在修改链表的时候，我们需要对链表进行加锁
	hotThreshold     float32                  // 热数据区域的阈值
	hotDataThreshold time.Duration            // 如果一个缓存页处于热数据区域，且在热数据区域的前 1/4，那么访问这个缓存页的时候，就不需要移动到热数据区的头部,1s为基础点
}

func NewLRUCache(cap int, hotDataThreshold time.Duration, hotThreshold float32) *LRUCache {
	return &LRUCache{
		capacity:         cap,
		cache:            make(map[string]*list.Element),
		hotDataList:      list.New(),
		coldDataList:     list.New(),
		hotDataThreshold: hotDataThreshold,
		hotThreshold:     hotThreshold,
	}
}

// Get 从缓存中读取数据
func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.cache[key]; ok { //读取到了缓存
		item := elem.Value.(*CacheItem) //从缓存中得到相应的节点元素
		if !c.isHotData(elem) {         //判断当前是不是热点数据
			//当前不是热数据，同时超过指定秒，就需要移动到热链表的头部
			c.moveToHotArea(elem)
		}
		return item.value, true
	} else {
		return nil, false
	}
}

// Set 往缓存中写入数据
//插入的时候首先插入到冷链表中
func (c *LRUCache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.cache[key]; ok {
		// 缓存命中了，更新缓存项的值，并将其移动到热数据区头部
		item := elem.Value.(*CacheItem)
		item.value = value
		if !c.isHotData(elem) {
			c.moveToHotArea(elem)
		}
	} else {
		// 缓存未命中，新建一个缓存项，并将其插入到 LRU 链表头部
		item := &CacheItem{
			key:        key,
			value:      value,
			insertTime: time.Now(),
			loocation:  COLD,
		}
		if len(c.cache) == c.capacity {
			c.evict()
		}
		elem := c.coldDataList.PushFront(item) //将新来的元素存放到冷链表的头部中
		c.cache[key] = elem                    //更新缓存
		// 如果缓存区已满，需要淘汰最近最少使用的缓存项

	}
}

// isHotArea 判断一个元素是否处于热数据区域
func (c *LRUCache) isHotData(elem *list.Element) bool {
	//如果当前的时间小于1s，就说明他不是一个热点数据（可能是偶然访问）不需要移动，如果当前数据访问时间超过1s，说明他是热点数据，就需要将他移动到热数据的头部
	//如果当前数据处在热lrulist中，同时处在热数据的前1/4中，就不用移动，否则就就需要移动到热点中
	idx := 1
	if c.findList(elem) == c.hotDataList {
		//判断是不是在热点数据的前1/4
		//遍历这个链表
		for e := c.hotDataList.Front(); e != nil; e = e.Next() {
			fmt.Println(e.Value)
			if e.Value == elem.Value {
				less := float32(idx)/float32(c.hotDataList.Len()) < c.hotThreshold
				return less
			}
			idx++
		}
	}
	return false
}

// moveToHotArea 将一个元素移动到热数据区域头部
func (c *LRUCache) moveToHotArea(elem *list.Element) {
	isHot := c.findList(elem) == c.hotDataList
	if time.Since(elem.Value.(*CacheItem).insertTime) > c.hotDataThreshold || isHot {
		//如果是热数据，就直接移动到热链表的头部
		if isHot {
			c.hotDataList.MoveToFront(elem)
			return
		} else {
			//如果是冷数据就需要将他删除，并移动到热数据的头部
			c.coldDataList.Remove(elem) //删除
			elem.Value.(*CacheItem).loocation = HOT
			e := c.hotDataList.PushFront(elem.Value) //将他插入到热链表的头部
			delete(c.cache, elem.Value.(*CacheItem).key)
			c.cache[e.Value.(*CacheItem).key] = e
		}
	}

}

// evict 淘汰最近最少使用的缓存项，从冷链表中删除
func (c *LRUCache) evict() {
	for len(c.cache) == c.capacity {
		if c.coldDataList.Len() != 0 {
			c.remove(c.coldDataList)
		} else if c.hotDataList.Len() != 0 {
			c.remove(c.hotDataList)
		}

	}
}

func (c *LRUCache) remove(l *list.List) {
	elem := l.Back()
	item := l.Remove(elem).(*CacheItem)
	delete(c.cache, item.key)
}
func (c *LRUCache) findList(elem *list.Element) *list.List {
	if elem.Value.(*CacheItem).loocation == COLD {
		return c.coldDataList
	} else if elem.Value.(*CacheItem).loocation == HOT {
		return c.hotDataList
	}
	return nil
}

func isEqual(a, b *list.Element) bool {
	if a == nil || b == nil {
		return false
	} //todo 从链表中取出数据并解读，出错
	itemA, okA := a.Value.(*CacheItem)
	itemB, okB := b.Value.(*CacheItem)
	if !okA || !okB {
		return false
	}
	return itemA.key == itemB.key
}

// Remove 从LRU缓存中移除指定的缓存页
func (c *LRUCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		list := c.findList(elem)
		list.Remove(elem)
		delete(c.cache, key)
	}
}

//Contain 判断某一个区块是否存在
func (c *LRUCache) Contain(key string) bool {
	_, ok := c.cache[key]
	return ok //判断某一个区块是否存在
}

func (c *LRUCache) Items() map[string]*list.Element {
	return c.cache
}
