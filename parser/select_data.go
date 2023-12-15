package parser

import "miniSQL/query"

/*
	SQL解析完之后，会创建一个QueryData对象，我们接下来就是需要根据这个对象构建出合适的查询规划器Planner
	采取从简单到复杂的原则，首先我们直接构建QueryData的信息去构建查询规划对象，此时我们不用考虑他构造出来的查询树是否足够优化
	慢慢改进算法，直到构建出一个足够优秀的查询树

	在笛卡尔积中每一条记录都和另一个表中的所有记录进行合并匹配
*/

//QueryData 保存query查询解析出来的结果,在预处理器在中会对这里面的字段和表进行检查是否存在
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

//ToString 将这个SQL语句转化成字符串的形式
func (q *QueryData) ToString() string {
	result := "SELECT "
	fieldNum := len(q.fields)

	for i, fldName := range q.fields {
		result += fldName
		if i != fieldNum-1 {
			result += ", "
		}
	}
	result = result[:len(result)-1]
	result += " FROM "
	tableNum := len(q.tables)
	for i, tableName := range q.tables {
		result += tableName
		if i != tableNum-1 {
			result += ", "
		}
	}
	//result = result[:len(result)-1]
	predStr := q.pred.ToString()
	if predStr != "" {
		result += " WHERE " + predStr
	}
	return result
}
