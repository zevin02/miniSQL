package record_manager

import (
	"fmt"
	"testing"
)

func TestSchema(t *testing.T) {
	sch := NewSchema()                 //构造一个表对象
	sch.AddIntField("A")               //添加一个field为int类型
	sch.AddStringField("B", len("B"))  //添加一个field为string类型
	layout := NewLayoutWithSchema(sch) //获得某个记录
	for _, fieldName := range layout.Schema().Fields() {
		//遍历schema中的每个字段类型
		offset := layout.Offset(fieldName)                   //获得某个字段在表中的偏移量
		fmt.Printf("%s has offset %d \n", fieldName, offset) //把field名和他的偏移量都打赢出来

	}
}
