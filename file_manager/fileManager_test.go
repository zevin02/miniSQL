package file_manager

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFileManager(t *testing.T) {
	fm, err := NewFileManager("/home/zevin/file_test", 400)
	assert.Nil(t, err)
	blk := NewBlockId("testfile", 2)    //管理的文件是目录下的testfile文件，区块号是2
	p1 := NewPageBySize(fm.BlockSize()) //开辟一个内存中的400字节的缓存
	pos1 := uint64(88)                  //在区块号的88位置进行写入
	s := "abcdefighiki"
	p1.SetString(pos1, s)            //先把数据写入到内存中的缓存中
	size := p1.MaxLengthForString(s) //得到大小
	//先写入到内存缓存中，在合适的时间再写入到磁盘文件中
	pos2 := pos1 + size //得到了下一个写入的位置
	val := int64(345)
	//写入一个整形
	p1.SetInt(pos2, val)
	fm.Write(blk, p1) //将p1中的数据写入到磁盘文件中
	//同样再读取的时候，也是先读取到缓存中，再从缓存中读取数据
	p2 := NewPageBySize(fm.BlockSize())
	fm.Read(blk, p2)
	assert.Equal(t, val, p2.GetInt(pos2))
	assert.Equal(t, s, p2.GetString(pos1))
}
