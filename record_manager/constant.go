package record_manager

//Constant 用户可以不用指定string或者int类型数据的插入
type Constant struct {
	Ival  int
	Sval  string
	isInt bool //判断当前对象是否是int类型
}

//NewConstantInt 构造当前位int类型的对象
func NewConstantInt(val int) *Constant {
	return &Constant{
		Ival:  val,
		isInt: true,
	}
}

//NewConstantString 构造当前位int类型的对象
func NewConstantString(val string) *Constant {
	return &Constant{
		Sval:  val,
		isInt: true,
	}
}
