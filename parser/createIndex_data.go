package parser

import "fmt"

type CreateIndexData struct {
	indexName string
	tableName string
	fields    []string
}

func NewIndexData(indexName string, tableName string, fields []string) *CreateIndexData {
	return &CreateIndexData{
		indexName: indexName,
		tableName: tableName,
		fields:    fields,
	}
}

func (i *CreateIndexData) IndexName() string {
	return i.indexName
}

func (i *CreateIndexData) TableName() string {
	return i.tableName
}

func (i *CreateIndexData) FieldName() []string {
	return i.fields
}

func (i *CreateIndexData) ToString() string {
	str := fmt.Sprintf("index name :%s ,table name %s,field name %v", i.indexName, i.tableName, i.fields)
	return str
}
