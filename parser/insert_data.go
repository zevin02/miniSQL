package parser

import "miniSQL/comm"

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
