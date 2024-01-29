package parser

import (
	"miniSQL/comm"
)

//InsertData 这个解析出来就是相当于抽象语法树
type InsertData struct {
	tableName string
	fields    []string
	values    []*comm.Constant
}

func NewInsertData(tblName string, fields []string, values []*comm.Constant) *InsertData {
	return &InsertData{
		tableName: tblName,
		fields:    fields,
		values:    values,
	}
}

func (d *InsertData) TableName() string {
	return d.tableName
}

func (d *InsertData) Fields() []string {
	return d.fields
}

func (d *InsertData) Vals() []*comm.Constant {
	return d.values
}
