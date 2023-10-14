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
	cost    float64      //查询树的成本,一开始是当前执行之前的大小，执行完Cost之后，才是当前完整的查询树的成本
}

//NewTablePlan 初始化当前的TablePlanner
func NewTablePlan(tx *tx.Transaction, tblName string, md *mm.MetaDataManager) (*TablePlan, error) {
	var err error
	tblPlanner := &TablePlan{
		tx:      tx,
		tblName: tblName,
	}
	//从元数据管理器中获得表结构layout信息
	if tblPlanner.layout, err = md.GetLayout(tblName, tx); err != nil {
		return nil, nil
	}
	//从元数据管理器中获得表的统计信息
	if tblPlanner.si, err = md.GetStatInfo(tblName, tblPlanner.layout, tx); err != nil {
		return nil, nil
	}

	//计算查询树的成本
	tblPlanner.cost = tblPlanner.Cost()
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

func (t *TablePlan) RecordsOutput() int {
	return t.si.RecordsOutput() //在当前的元数据管理器中查询该表的记录数
}

func (t *TablePlan) BlockAccessed() int {
	return t.si.BLockAccessed() //查询当前访问的区块格式
}

//DistinctValues V(s,F),调用底部的元数据管理器，获得每个字段有多少个唯一的值
func (t *TablePlan) DistinctValues(fldName string) int {
	return t.si.DistinctValue(fldName)
}

func (t *TablePlan) Schema() rm.SchemaInterface {
	return t.layout.Schema()
}

//Cost 计算当前步骤的开销
func (t *TablePlan) Cost() float64 {
	cost := float64(t.BlockAccessed())*ioCost + float64(t.RecordsOutput())*cpuCost //计算出当前的成本
	t.cost += cost
	return t.cost
}
