package planner

import (
	mm "miniSQL/metadata_manager"
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
)

//TablePlan 这个每个对应的planner就是用来测算对应的Planner
//tablePlan是查询树最底层的，他直接返回对应数据库表的统计信息
type TablePlan struct {
	tx      *tx.Transaction
	tblName string       //表名
	layout  *rm.Layout   //表的格式信息
	si      *mm.StatInfo //表的统计信息
}

//NewTablePlan 初始化当前的TablePlanner
func NewTablePlan(tx *tx.Transaction, tblName string, md *mm.MetaDataManager) (*TablePlan, error) {
	var err error
	tblPlanner := &TablePlan{
		tx:      tx,
		tblName: tblName,
	}
	//从元数据管理器中获得表结构
	if tblPlanner.layout, err = md.GetLayout(tblName, tx); err != nil {
		return nil, nil
	}
	//从元数据管理器中获得表的统计信息
	if tblPlanner.si, err = md.GetStatInfo(tblName, tblPlanner.layout, tx); err != nil {
		return nil, nil
	}
	return tblPlanner, nil
}

//Open 返回当前的tableScan对象
func (t *TablePlan) Open() (interface{}, error) {
	tbleScan, err := rm.NewTableScan(t.tx, t.tblName, t.layout)
	if err != nil {
		return nil, nil
	}
	return tbleScan, nil
}

func (t *TablePlan) RecordsOuput() int {
	return t.si.RecordsOutput() //在当前的元数据管理器中查询该表的记录数
}

func (t *TablePlan) BlockAccessed() int {
	return t.si.BLockAccessed() //查询当前访问的区块格式
}

//DistinctValues V(s,F)
func (t *TablePlan) DistinctValues(fldName string) int {
	return t.si.DistinctValue(fldName)
}

func (t *TablePlan) Schema() rm.SchemaInterface {
	return t.layout.Schema()
}
