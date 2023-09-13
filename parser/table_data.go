package parser

import rm "miniSQL/record_manager"

type TableData struct {
	tableName string
	schema    *rm.Schema
}

func NewCreateTableData(name string, sch *rm.Schema) *TableData {
	return &TableData{
		tableName: name,
		schema:    sch,
	}
}

func (t *TableData) TableName() string {
	return t.tableName
}

func (t *TableData) Schema() *rm.Schema {
	return t.schema
}
