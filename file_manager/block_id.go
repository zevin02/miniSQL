package file_manager

import (
	"crypto/sha256"
	"fmt"
)


,
"fmt"
)

//BlockId 用来描述一个二进制文件里面的一块内容
type BlockId struct {
	fileName string //对应磁盘的二进制文件
	blkNum   uint64 //二进制文件中区块的标号
}

//构造函数，构造一个BlockId对象
func NewBlockId(fileName string, blkNum uint64) *BlockId {
	return &BlockId{
		fileName: fileName,
		blkNum:   blkNum,
	}
}

//FileName 返回他的文件名
func (b *BlockId) FileName() string {
	return b.fileName
}

//Number 返回他的区块号码
func (b *BlockId) Number() uint64 {
	return b.blkNum
}

//Equal 判读两个BlockID对象是否相同
func (b BlockId) Equal(other *BlockId)bool  {
	return b.fileName==other.fileName&&b.blkNum==other.blkNum
}


//计算当前的BlockId的哈希值
func asSha256(o interface{})string{
	h:=sha256.New()//构造一个哈希对像
	h.Write([]byte(fmt.Sprintf("%v",o)))//将当前的接口对像进行写入哈希对象，将输入对像o转化成字符串的形式
	return fmt.Sprintf("%x",h.Sum(nil))//计算哈希值，把数据按照16进制的形式进行返回
}

func(b *BlockId)HashCode()string{
	return asSha256(*b)//传入当前的BlockId对象，进行处理
}
