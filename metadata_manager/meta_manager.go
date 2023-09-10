package metadata_manager

import (
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
)

//MetaDataManager 将实现的tablemgr，viewmgr,statmgr这些管理器全部管理起来
type MetaDataManager struct {
	tblmgr  *TableManager
	viewmgr *ViewManager
	statmgr *StatManager
	//索引管理器以后再做处理
}

//NewMetaDataManager 构造一个MetaDataManager对象,isnew=true说明当前的tableManager还没有创建出来，我们需要首先创建出来两张元数据表，同时视图管理器的表也没创建出来，我们也需要进行创建
func NewMetaDataManager(isNew bool, tx *tx.Transaction) (*MetaDataManager, error) {
	metaMgr := &MetaDataManager{}
	var err error
	metaMgr.tblmgr, err = NewTableManager(isNew, tx)
	if err != nil {
		return nil, err
	}
	metaMgr.viewmgr, err = NewViewManager(isNew, metaMgr.tblmgr, tx)
	if err != nil {
		return nil, err
	}
	metaMgr.statmgr, err = NewStatManager(metaMgr.tblmgr, tx)
	if err != nil {
		return nil, err
	}
	return metaMgr, nil
}

//CreateTable 创建一张表,通过调用底层的表管理器来创建表
func (m *MetaDataManager) CreateTable(tblname string, sch *rm.Schema, tx *tx.Transaction) error {
	err := m.tblmgr.CreateTable(tblname, sch, tx)
	if err != nil {
		return err
	}
	return nil
}

//CreateView 创建一张视图，通过底层的视图管理器来实现
func (m *MetaDataManager) CreateView(vname string, vdef string, tx *tx.Transaction) error {
	err := m.viewmgr.CreateView(vname, vdef, tx)
	if err != nil {
		return err
	}
	return nil
}

//GetLayout 得到某张表的表结构，通过调用底层的表管理器来实现
func (m *MetaDataManager) GetLayout(tblname string, tx *tx.Transaction) (*rm.Layout, error) {
	layout, err := m.tblmgr.GetLayout(tblname, tx)
	if err != nil {
		return nil, err
	}
	return layout, nil
}

//GetViewDef 得到创建某张表使用的SQL语句
func (m *MetaDataManager) GetViewDef(vname string, tx *tx.Transaction) (string, error) {
	viewDef, err := m.viewmgr.GetViewDef(vname, tx)
	if err != nil {
		return "", err
	}
	return viewDef, nil
}

//GetStatInfo 得到某张表的统计信息
func (m *MetaDataManager) GetStatInfo(tblname string, layout *rm.Layout, tx *tx.Transaction) (*StatInfo, error) {
	info, err := m.statmgr.GetStatInfo(tblname, layout, tx)
	if err != nil {
		return nil, err
	}
	return info, nil
}
