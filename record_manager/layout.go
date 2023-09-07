package record_manager

import (
	fm "miniSQL/file_manager"
	"miniSQL/transaction"
)

const (
	BYTES_OF_INT = 8 //一个INT占用的字节大小
)

//Layout 具体的描述某一条记录
type Layout struct {
	schema   SchemaInterface
	offsets  map[string]int //每个字段在记录中的偏移
	slotSize int            //这条记录的长度
}

//NewLayoutWithSchema 使用schema来初始化一个记录
func NewLayoutWithSchema(schema SchemaInterface) *Layout {
	layout := &Layout{
		schema:   schema,
		offsets:  make(map[string]int),
		slotSize: 0,
	}
	fields := schema.Fields()       //获得当前表中的所有字段
	pos := transaction.UIN64_LENGTH //最开始的8个字节用来做占用符，0表示当前slot没有使用，1表示有使用
	//遍历一下这里面的所有字段
	for i := 0; i < len(fields); i++ {
		layout.offsets[fields[i]] = pos        //更新当前field对应在记录中的偏移量
		pos += layout.lengthInBytes(fields[i]) //加上当前字段占用的字节数,更新到当前字段的下一个偏移位置
	}
	layout.slotSize = pos //这样就获得某条记录的长度
	return layout
}

//NewLayout 初始化当前的layout记录对象
func NewLayout(schema SchemaInterface, offsets map[string]int, slotSize int) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

//Schema 直接放回当前的记录对应的是哪一个表的记录
func (l *Layout) Schema() SchemaInterface {
	return l.schema
}

//Offset 返回这个字段在这个表中的偏移,如果当前field不存在的话，就返回-1,否则就返回正常的值
func (l *Layout) Offset(fieldName string) int {
	offset, ok := l.offsets[fieldName]
	if !ok {
		return -1
	}
	return offset
}

//SlotSize 返回某个记录占用了多少个字节
func (l *Layout) SlotSize() int {
	return l.slotSize
}

//lengthInBytes 某个field占用的字节大小
func (l *Layout) lengthInBytes(fieldName string) int {
	fieldType := l.schema.Type(fieldName) //从表中获得该field的类型
	p := fm.NewPageBySize(1)
	if fieldType == INTEGER {
		return BYTES_OF_INT
	} else {
		fieldLen := l.schema.Length(fieldName) //获得某个field的长度
		/*
			因为是varchar类型，我们根据长度构造一个字符串，调用page.maxLengthForString,获得写入页面的数据长度,8字节保存字符串的长度

		*/
		dummyStr := string(make([]byte, fieldLen))
		return int(p.MaxLengthForString(dummyStr)) //返回他在page中管理的实际长度8+length
	}
}
