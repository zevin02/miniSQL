package parser

import "miniSQL/query"

type DeleteData struct {
	tblName string
	pred    *query.Predicate
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
