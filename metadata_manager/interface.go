package metadata_manager

import (
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
)

type TableManagerInterface interface {
	//CreateTable 创建一张表，并添加到tbcat和fldcat两张表进行管理
	CreateTable(tblName string, schema *rm.Schema, tx *tx.Transaction) error
	//GetLayout 获得某张表的表结构
	GetLayout(tblName string, tx *tx.Transaction) (*rm.Layout, error)
}
