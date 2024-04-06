package metadata_manager

import (
	"miniSQL/comm"
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
)

type TableManagerInterface interface {
	//CreateTable 创建一张表，并添加到tbcat和fldcat两张表进行管理
	CreateTable(tblName string, schema *rm.Schema, tx *tx.Transaction) error
	//GetLayout 获得某张表的表结构
	GetLayout(tblName string, tx *tx.Transaction) (*rm.Layout, error)
}

//Index 创建一个index接口对象,后续可以基于这个实现不同的索引算法，hash或者b+树
type Index interface {
	BeforeFirst(searchKey *comm.Constant)
	Next() bool
	Close()
	GetDataRID() *rm.RID
	Insert(val *comm.Constant, rid *rm.RID)
	Delete(val *comm.Constant, rid *rm.RID)
}
