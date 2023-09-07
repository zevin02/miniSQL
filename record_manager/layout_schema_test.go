package record_manager

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	tx "miniSQL/transaction"
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

/*create table s
(age int,name string,height int)

*/
func TestLayout_Offset(t *testing.T) {
	sch := NewSchema() //创建一个表对象
	//往里面添加一些字段
	sch.AddIntField("age")
	sch.AddStringField("name", 9)
	sch.AddIntField("height")
	layout := NewLayoutWithSchema(sch) //传入schema得到每个字段在这个schema的偏移位置
	fields := sch.Fields()             //获得当前表的所有字段
	offsetAge := layout.Offset(fields[0])
	assert.Equal(t, tx.UIN64_LENGTH, offsetAge) //因为最开始的头8字节存储了这个字段是否有效
	offsetName := layout.Offset(fields[1])
	assert.Equal(t, 16, offsetName)          //height
	offsetHeight := layout.Offset(fields[2]) //
	assert.Equal(t, 33, offsetHeight)        //height
	assert.Equal(t, 41, layout.SlotSize())
}
