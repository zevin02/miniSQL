package file_manager

import (
	"encoding/binary"
)

//page对象是系统中的一块内存，将BlockId对应的数据，映射到内存中

type Page struct {
	buffer []byte //内存中的一块数据,和BlockId上对应的数据是有映射关系的

}

//NewPageBySize 构造Page对象
func NewPageBySize(blockSize uint64) *Page {
	buf := make([]byte, blockSize) //根据blockSize要求的大小，来开辟缓存区
	return &Page{
		buffer: buf,
	}
}

//NewPageByBytes 根据Bytes传入来直接赋值，构造该对象
func NewPageByBytes(bytes []byte) *Page {
	return &Page{
		buffer: bytes, //根据传入的缓冲区直接进行赋值
	}
}

//GetInt 根据offset从缓冲区里面读取一个uint64的数据出来
func (p *Page) GetInt(offset uint64) uint64 {
	num := binary.LittleEndian.Uint64(p.buffer[offset : offset+8]) //读取出来一个8字节大小的数
	return num
}

//将一个64位数编码成一个字节数组
func Uint64ToByteArray(val uint64) []byte {
	b := make([]byte, 8)                  //开辟一个8字节大小的空间出来
	binary.LittleEndian.PutUint64(b, val) //把数据进行编码写入到b缓冲区中
	return b
}

//设置page中的缓冲区
func (p *Page) SetInt(offset, val uint64) {
	b := Uint64ToByteArray(val) //把val进行编码缓存到b中
	copy(p.buffer[offset:], b)  //把编码好的数据缓存到buffer中
}

//GetBytes 在给定的偏移中，从Page的Buf中读取出来一个我们需要的字节数组
func (p *Page) GetBytes(offset uint64) []byte {
	//buf的一个字符串写入，前面的8个字节是当前字符串的长度，我们每次读取数据，都需要先获得相应的数据的长度
	len := binary.LittleEndian.Uint64(p.buffer[offset : offset+8]) //先读取数组的长度
	newBuf := make([]byte, len)
	copy(newBuf, p.buffer[offset+8:])
	return newBuf
}

//SetBytes 往Page中的buf中写入相应的字节
func (p *Page) SetBytes(offset uint64, b []byte) {
	//写入的时候，首先要写入的就是数组的长度
	length := uint64(len(b))
	lenBuf := Uint64ToByteArray(length) //将当前的俄长度
	//然后再将编码后的长度字节数组写入到缓冲区中
	copy(p.buffer[offset:], lenBuf)
	//再把数据拷贝进去
	//可能buffer中的数据不够b进行拷贝，可能会出现bug
	copy(p.buffer[offset+8:], b)
}

//GetString 从缓冲区中读取出来一个字符串
func (p *Page) GetString(offset uint64) string {
	strBytes := p.GetBytes(offset) //先获得字节数组
	return string(strBytes)
}

//SetString 将一个字符串写入到缓冲区中
func (p *Page) SetString(offset uint64, s string) {
	//先将字符串转化成字节数组
	strBytes := []byte(s)
	//将当前的字节数组进行写入
	p.SetBytes(offset, strBytes) //先获得字节数组
}

//MaxLengthForString 返回输入字符串的长度
func (p *Page) MaxLengthForString(s string) uint64 {
	//hello,世界  ---len=13
	uint64Length := 8
	return uint64(uint64Length + len(s)) //8字节+字符占用的长度=编码时用到的总共的字节数
}

//将buffer中的数据全部返回
func (p *Page) Contents() []byte {
	return p.buffer
}
