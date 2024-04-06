package comm

import (
	"hash/fnv"
	"math/big"
	"strconv"
)

//Constant 用户可以不用指定string或者int类型数据的插入,这个可以表示一个常量
type Constant struct {
	Ival *int
	Sval *string
}

//NewConstantInt 构造当前位int类型的对象
func NewConstantInt(val *int) *Constant {
	return &Constant{
		Ival: val,
		Sval: nil,
	}
}

//NewConstantString 构造当前位int类型的对象
func NewConstantString(val *string) *Constant {
	return &Constant{
		Sval: val,
		Ival: nil,
	}
}

//ToString 将该Constant存储的值按照字符串的形式显示
func (c *Constant) ToString() string {
	if c.Ival != nil {
		//当前是int类型
		return strconv.FormatInt((int64)(*c.Ival), 10) //将他转化string类型
	}
	return *c.Sval
}

//AsInt 将当前的Constant类型作为int类型返回
func (c *Constant) AsInt() int {
	return *c.Ival
}

//AsString 将当前Constant类型作为string类型返回
func (c *Constant) AsString() string {
	return *c.Sval
}

//Equal	判断两个Constant类型是否相同
func (c *Constant) Equal(obj *Constant) bool {
	//判断两个Constant类型是否相同
	if c.Ival != nil && obj.Ival != nil {
		return *c.Ival == *obj.Ival
	}
	if c.Sval != nil && obj.Sval != nil {
		return *c.Sval == *obj.Sval
	}
	return false
}

//HashCode 获得他的一个哈希值
func (c *Constant) HashCode() uint32 {
	var bytes []byte
	h := fnv.New32a()
	if c.Ival != nil {
		//将数值转化成字节数组，然后再进行编码
		s := big.NewInt(int64(*c.Ival)) //转化成一个Int类型的变量
		bytes = s.Bytes()               //将他转化成一个字节数组
	} else {
		bytes = []byte(*c.Sval) //如果是字符串类型，就可以直接将他转化成一个字节数组
	}
	h.Write(bytes) //写入到这个对象中去
	//根据写入的数据，生成一个哈希值
	return h.Sum32()
}
