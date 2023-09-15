package container

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLRUCache1(t *testing.T) {
	// 创建一个容量为 2 的 LRU 缓存
	cache := NewLRUCache(2, time.Second, 1/4)

	// 写入缓存
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")

	// 读取缓存
	value, ok := cache.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", value)
	time.Sleep(2 * time.Second)
	// 再次写入缓存
	cache.Set("key3", "value3")
	assert.Equal(t, 2, cache.coldDataList.Len())
	time.Sleep(1 * time.Second)

	value, ok = cache.Get("key3")

	assert.True(t, ok)
	assert.Equal(t, "value3", value)
	assert.Equal(t, 1, cache.coldDataList.Len())
	assert.Equal(t, 1, cache.hotDataList.Len())

	// 读取缓存
	_, ok = cache.Get("key2")

	assert.True(t, ok)
	assert.Equal(t, 0, cache.coldDataList.Len())
	assert.Equal(t, 2, cache.hotDataList.Len())
}

func TestLRUCache2(t *testing.T) {
	cache := NewLRUCache(9, time.Second, 0.25)
	for i := 0; i < 9; i++ {
		cache.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}
	assert.Equal(t, 9, cache.coldDataList.Len())
	assert.Equal(t, 0, cache.hotDataList.Len())
	cache.Set(fmt.Sprintf("key%d", 9), fmt.Sprintf("value%d", 9))
	assert.Equal(t, "value9", cache.coldDataList.Front().Value.(*CacheItem).value)
	assert.NotEqual(t, "value0", cache.coldDataList.Back().Value.(*CacheItem).value)
	time.Sleep(2 * time.Second)
	for i := 1; i < 10; i++ {
		value, ok := cache.Get(fmt.Sprintf("key%d", i))
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprintf("value%d", i), value)
	}
	//移动到热链表中
	assert.Equal(t, 0, cache.coldDataList.Len())
	assert.Equal(t, 9, cache.hotDataList.Len())
	assert.Equal(t, "value9", cache.hotDataList.Front().Value.(*CacheItem).value)
	//处在热区的前1/4，不移动
	cache.Get("key8")
	assert.NotEqual(t, "value8", cache.hotDataList.Front().Value.(*CacheItem).value)
	assert.Equal(t, "value9", cache.hotDataList.Front().Value.(*CacheItem).value)
	//访问非热区的前1/4,就移动到热区

	cache.Get("key1")
	assert.Equal(t, "value1", cache.hotDataList.Front().Value.(*CacheItem).value)
	cache.Set(fmt.Sprintf("key%d", 11), fmt.Sprintf("value%d", 11))

}
