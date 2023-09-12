package parser

import "miniSQL/query"

type QueryData struct {
	fields []string
	tables []string
	pred   *query.Predicate //这个是条件
}

func NewQueryData(fields []string, tables []string, pred *query.Predicate) *QueryData {
	return &QueryData{
		fields: fields,
		tables: tables,
		pred:   pred,
	}
}

func (q *QueryData) Fields() []string {
	return q.fields
}

func (q *QueryData) Tables() []string {
	return q.tables
}

func (q *QueryData) Pred() *query.Predicate {
	return q.pred
}

//将这个SQL语句转化成字符串的形式
func (q *QueryData) ToString() string {
	result := "SELECT "
	fieldNum := len(q.fields)

	for i, fldName := range q.fields {
		result += fldName
		if i != fieldNum-1 {
			result += ", "
		}
	}

	// 去掉最后一个逗号
	result = result[:len(result)-1]
	result += " FROM "
	tableNum := len(q.tables)
	for i, tableName := range q.tables {
		result += tableName
		if i != tableNum-1 {
			result += ", "
		}
	}
	// 去掉最后一个逗号

	//result = result[:len(result)-1]
	predStr := q.pred.ToString()
	if predStr != "" {
		result += " WHERE " + predStr
	}

	return result
}
