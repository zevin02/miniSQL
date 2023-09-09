package record_manager

import fm "miniSQL/file_manager"

//SchemaInterface 某个表的描述
type SchemaInterface interface {
	//给表添加一个字段
	//字段有两种类型，一种是string，一种是int类型
	AddField(fieldName string, fieldType FIELD_TYPE, length int)
	AddIntField(fieldName string)
	AddStringField(fieldName string, length int)
	Add(fieldName string, sch SchemaInterface) //整形类型或字符串类型都能添加
	AddAll(sch SchemaInterface)                //把该对象的所有描述都加入进来
	Fields() []string                          //返回所有字段的名称
	HashField(fieldName string) bool           //是否有某个字段
	Type(fieldName string) FIELD_TYPE          //返回某个字段对应的类型z
	Length(fieldName string) int               //返回该字段对应列的长度
}

//LayoutInterface 某字字段在表中的偏移
// 8   16  1
//1000,张三,男，计算机科学技术(departMent)
//layoutinterface.offset(department)=8+16+1=25放回某个列的偏移
//用来描述某个具体的字段
type LayoutInterface interface {
	Schema() SchemaInterface
	Offset(fieldName string) int //返回这个字段在这个表中的偏移
	SlotSize() int               //返回某个记录占用了多少个字节
}

//RecordManagerInterface 记录管理器
type RecordManagerInterface interface {
	Block() *fm.BlockId                               //当前记录处在的哪个文件块中
	GetInt(slot int, fieldName string) int            //返回该字段的值,给定记录所在的编号和记录的field
	SetInt(slot int, fieldName string, val int)       //给某个字段设置数据
	GetString(slot int, fieldName string) string      //返回该字段的值,给定记录所在的编号和记录的field
	SetString(slot int, fieldName string, val string) //给某个字段设置string类型数据
	Format()                                          //将所有页面内的记录设置为默认值
	Delete(slot int)                                  //删除给定编号的记录,只需要把这个占位符设置为无效即可,设置成0
	//某一条记录都有一个占位符来表示这个记录是否有效
	NextAfter(slot int) int   //给出从给定编号之后，flag标志位被设置成1(有效的)的记录的编号
	InsertAfter(slot int) int //查找给定编号在之后，flag标志设置成0（无效）记录的编号,可以使用该位置进行设置记录

}

//RIDInterface 区块槽位管理
type RIDInterface interface {
	BlockNumber() int //记录所在的区块号
	Slot() int        //记录的插槽号
	Equals(other RIDInterface) bool
	ToString() string
}

type TableScanInterface interface {
	Close()
	HasField(fieldName string) bool
	BeforeFirst()             //将指针放在第一条记录前
	Next() bool               //读取下一条记录
	MoveToRid(r RIDInterface) //跳转到给定目录
	Insert()

	GetInt(fieldName string) int
	GetString(fieldName string) string
	SetInt(fieldName string, val int)
	SetString(fieldName string, val string)
	GetRid() RIDInterface
	Delete()
}
