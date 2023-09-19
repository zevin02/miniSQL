package buffer_manager

import (
	"github.com/stretchr/testify/assert"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
	"testing"
	"time"
)

func TestBufferManager(t *testing.T) {
	fileManager, err := fm.NewFileManager("/home/zevin/buffer_test", 400) //打开一个文件管理器来管理文件
	assert.Nil(t, err)
	logManager, err := lm.NewLogManager(fileManager, "logfile")
	assert.Nil(t, err)
	assert.NotNil(t, logManager)
	bm := NewBufferManager(fileManager, logManager, 3) //缓存池中有3个缓存页面
	buff1, err := bm.Pin(fm.NewBlockId("testfile", 1)) //我们首先先读取第一个区块的内容
	assert.Nil(t, err)

	p := buff1.Contents() //先把缓存读取出来
	n := p.GetInt(80)     //读取当前页面的数据出来
	p.SetInt(80, n+1)     //修改该页面的数据
	buff1.SetModify(1, 0) //通知缓存管理器数据已经被修改了
	bm.Unpin(buff1)
	//把buffer1的数据给unpin掉

	buff2, err := bm.Pin(fm.NewBlockId("testfile", 2)) //我们再读取第2个区块的内容,因为上面把buff1给unpin掉，那么这个buff1的块就空出来了，就可以被使用了，当前buff2去获得buff块的时候，得到的就是这个buff1的区块，这个时候，就会把这个buff1的区块的数据给刷新到磁盘中
	assert.Nil(t, err)

	_, err = bm.Pin(fm.NewBlockId("testfile", 3)) //我们再读取第3个区块的内容
	assert.Nil(t, err)

	//这样的话buff4的数据就能够pin成功了，但是由于前面的缓存有数据，会导致buff1把数据写入到磁盘中
	_, err = bm.Pin(fm.NewBlockId("testfile", 4)) //缓存页面已经使用完了，这里分配应该返回错误
	assert.Nil(t, err)

	bm.Unpin(buff2) //由于buff2中的数据没有被修改，所以unpin不会将数据写入到磁盘中

	buff4, err := bm.Pin(fm.NewBlockId("testfile", 1)) //当前区块已经存在了，所以会直接增加引用计数，直接获得该区块，而不用去分配
	assert.Nil(t, err)
	p2 := buff4.Contents() //先获得当前区块的内容
	n1 := p2.GetInt(80)
	assert.Equal(t, int64(1), n1)
	p2.SetInt(80, 9999)
	buff4.SetModify(1, 0) //当前修改了，所以需要使用这个
	bm.Unpin(buff4)       //这里面的数据不会写入到磁盘中的

	//把testfile的区块1的数据写入到磁盘中，确认buff1的修改写入到磁盘
	page := fm.NewPageBySize(400)
	b1 := fm.NewBlockId("testfile", 1) //读取前面的buffer1的数据
	fileManager.Read(b1, page)
	n1 = page.GetInt(80)
	assert.Equal(t, int64(9999), n1)
	_, err = bm.Pin(fm.NewBlockId("testfile", 5)) //缓存页面已经使用完了，这里分配应该返回错误
	assert.Nil(t, err)
	_, err = bm.Pin(fm.NewBlockId("testfile", 6)) //缓存页面已经使用完了，这里分配应该返回错误
	assert.NotNil(t, err)
}
func TestBufferManager2(t *testing.T) {
	fileManager, err := fm.NewFileManager("/home/zevin/buffer_test", 400) //打开一个文件管理器来管理文件
	assert.Nil(t, err)
	logManager, err := lm.NewLogManager(fileManager, "logfile")
	assert.Nil(t, err)
	assert.NotNil(t, logManager)
	bm := NewBufferManager(fileManager, logManager, 10) //缓存池中有3个缓存页面
	buff1, err := bm.Pin(fm.NewBlockId("testfile", 1))  //我们首先先读取第一个区块的内容
	assert.NotNil(t, buff1)
	buff2, err := bm.Pin(fm.NewBlockId("testfile", 2)) //我们首先先读取第一个区块的内容
	assert.NotNil(t, buff2)

	buff3, err := bm.Pin(fm.NewBlockId("testfile", 3)) //我们首先先读取第一个区块的内容
	assert.NotNil(t, buff3)
	buff4, err := bm.Pin(fm.NewBlockId("testfile", 4)) //我们首先先读取第一个区块的内容
	assert.NotNil(t, buff4)
	buff5, err := bm.Pin(fm.NewBlockId("testfile", 5)) //我们首先先读取第一个区块的内容
	assert.NotNil(t, buff5)
	buff6, err := bm.Pin(fm.NewBlockId("testfile", 6)) //我们首先先读取第一个区块的内容
	assert.NotNil(t, buff6)
	buff7, err := bm.Pin(fm.NewBlockId("testfile", 7)) //我们首先先读取第一个区块的内容
	assert.NotNil(t, buff7)
	buff8, err := bm.Pin(fm.NewBlockId("testfile", 8)) //我们首先先读取第一个区块的内容
	assert.NotNil(t, buff8)
	buff9, err := bm.Pin(fm.NewBlockId("testfile", 9))   //我们首先先读取第一个区块的内容
	buff10, err := bm.Pin(fm.NewBlockId("testfile", 10)) //我们首先先读取第一个区块的内容
	assert.NotNil(t, buff10)
	time.Sleep(2 * time.Second)
	buff11, err := bm.Pin(fm.NewBlockId("testfile", 9)) //我们首先先读取第一个区块的内容
	assert.NotNil(t, buff11)
	assert.Equal(t, buff9, buff11)
	for i := 1; i < 9; i++ {
		bm.Pin(fm.NewBlockId("testfile", uint64(i)))
	}
	bm.Pin(fm.NewBlockId("testfile", uint64(7)))
	bm.Unpin(buff10)
	bm.Pin(fm.NewBlockId("testfile", uint64(11)))
}
