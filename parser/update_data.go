package parser

import (
	"miniSQL/query"
)

//UpdateData 这个语法树中记录的就是需要进行update的SQL语句
type UpdateData struct {
	tableName string
	fieldName string
	newVal    *query.Expression
	pred      *query.Predicate
}

func NewUpdateData(tblName string, fldName string, newVal *query.Expression, pred *query.Predicate) *UpdateData {
	return &UpdateData{
		tableName: tblName,
		fieldName: fldName,
		newVal:    newVal,
		pred:      pred,
	}

}
func (m *UpdateData) TableName() string {
	return m.tableName
}

func (m *UpdateData) TargetField() string {
	return m.fieldName
}

func (m *UpdateData) NewValue() *query.Expression {
	return m.newVal
}

func (m *UpdateData) Pred() *query.Predicate {
	return m.pred
}
