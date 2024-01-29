package parser

import rm "miniSQL/record_manager"

type CreateTableData struct {
	tableName string
	schema    *rm.Schema
}

func NewCreateTableData(name string, sch *rm.Schema) *CreateTableData {
	return &CreateTableData{
		tableName: name,
		schema:    sch,
	}
}

func (t *CreateTableData) TableName() string {
	return t.tableName
}

func (t *CreateTableData) Schema() *rm.Schema {
	return t.schema
}
