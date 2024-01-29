package parser

import "miniSQL/query"

//DeleteData 这个语法树中记录的delete的SQL语句
type DeleteData struct {
	tblName string           //删除的表
	pred    *query.Predicate //删除的条件
}

func NewDeleteData(tblName string, pred *query.Predicate) *DeleteData {
	return &DeleteData{
		tblName: tblName,
		pred:    pred,
	}

}

func (d *DeleteData) TableName() string {
	return d.tblName
}

func (d *DeleteData) Pred() *query.Predicate {
	return d.pred
}
