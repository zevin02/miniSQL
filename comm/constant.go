package comm

import "strconv"

//Constant 用户可以不用指定string或者int类型数据的插入
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
