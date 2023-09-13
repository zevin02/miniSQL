package query

import (
	"miniSQL/comm"
	rm "miniSQL/record_manager"
)

/*
	select name,age,sex from student where age=20
	Plan组件，将student这表从硬盘中取出来（把所有的列都取出来）,B+树，B树
	Project组件：从所有row中把需要的列取出来
	Select组建：在列的基础上，把age=20的都取出来

*/

/*
	SQL执行返回的结果可能对应数据库表里面的记录，也可能返回的是视图，他实际上是数据记录经过特定处理后的，并不对应磁盘上的数据
	因此像select执行的就能改，而insert,update执行后的结果就不能改

*/

//Scan 只能读取某张表
type Scan interface {
	// select name ,age from student where age>20,把student这张表的每一条记录都提取出来，再把age>20的记录都取出来，再取出给定的字段
	BeforeFirst() //把该对象重新指向起点
	Next() bool   //判断是否还有下一条
	GetInt(fieldName string) int
	GetString(fieldName string) string
	GetVal(fieldName string) *comm.Constant
	HasField(fieldName string) bool //判断某个字段是否存在这个表中
	Close()
}

//UpdateScan 修改SQL执行后的结果
type UpdateScan interface {
	//GetScan() Scan //继承上面的Scan的接口
	Scan
	SetInt(fieldName string, val int)
	SetString(fieldName string, val string)
	SetVal(fieldName string, val *comm.Constant)
	Insert()                      //在当前记录指向的位置进行插入
	Delete()                      //将当前指向的位置进行删除
	GetRid() rm.RIDInterface      //获得当前所在的区块槽位
	Move2Rid(rid rm.RIDInterface) //移动到指定的位置
}
