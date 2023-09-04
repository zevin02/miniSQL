package file_manager

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetAndGetInt(t *testing.T) {
	page := NewPageBySize(256) //开辟一个256的内存
	val := int64(1234)
	offset := uint64(23)
	page.SetInt(offset, val)      //把val数据写入到offset位置
	valGot := page.GetInt(offset) //读取一个int64大小的数据
	assert.Equal(t, val, valGot)

}

func TestSetAndGetBytesArray(t *testing.T) {
	page := NewPageBySize(256) //开辟一个256的内存
	bs := []byte{1, 23, 4, 5, 6}
	offset := uint64(111)
	page.SetBytes(offset, bs) //把数据写入到内存
	bsGot := page.GetBytes(offset)
	assert.Equal(t, bs, bsGot)
}

func TestSetAndGetString(t *testing.T) {
	page := NewPageBySize(256) //开辟一个256的内存
	s := "hello,世界"
	offset := uint64(25)
	page.SetString(offset, s)
	sGot := page.GetString(offset)
	assert.Equal(t, s, sGot)
}

//检验数据的编码后的实际最大长度
func TestMaxLengthForString(t *testing.T) {
	s := "hello,世界"
	sLen := uint64(len([]byte(s)))
	page := NewPageBySize(256)
	sLenGot := page.MaxLengthForString(s)
	assert.Equal(t, sLen+8, sLenGot) //获得实际的长度,实际的数据+8个长度字节
}

//检验Page中的数据是否相同
func TestGetContent(t *testing.T) {
	bs := []byte{1, 2, 3, 45, 2}
	page := NewPageByBytes(bs)
	bsGot := page.Contents()
	assert.Equal(t, bs, bsGot)
}
