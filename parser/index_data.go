package parser

import "fmt"

type IndexData struct {
	indexName string
	tableName string
	fields    []string
}

func NewIndexData(indexName string, tableName string, fields []string) *IndexData {
	return &IndexData{
		indexName: indexName,
		tableName: tableName,
		fields:    fields,
	}
}

func (i *IndexData) IndexName() string {
	return i.indexName
}

func (i *IndexData) TableName() string {
	return i.tableName
}

func (i *IndexData) FieldName() []string {
	return i.fields
}

func (i *IndexData) ToString() string {
	str := fmt.Sprintf("index name :%s ,table name %s,field name %v", i.indexName, i.tableName, i.fields)
	return str
}
