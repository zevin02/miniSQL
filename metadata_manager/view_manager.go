package metadata_manager

import (
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
)

/*
	视图是一张虚拟的表，他并不存在磁盘中，是通过一个或者多个表的查询结构生成
*/

const (
	//MAX_VIEWDEF 用于创建视图的SQL语句最大长度
	MAX_VIEWDEF = 100
)

//ViewManager 视图管理器
type ViewManager struct {
	tblgr *TableManager
}

//NewViewManager 创建一个视图管理器
func NewViewManager(isNew bool, tblgr *TableManager, tx *tx.Transaction) (*ViewManager, error) {
	viewManager := &ViewManager{
		tblgr: tblgr,
	}
	if isNew {
		//当前的视图管理器还没有被创建出来
		sch := rm.NewSchema()
		sch.AddStringField("viewname", MAX_NAME)     //当前视图的名字
		sch.AddStringField("viewdef", MAX_VIEWDEF)   //当前视图定义其使用的sql语句
		err := tblgr.CreateTable("viewcat", sch, tx) //创建一张viewcat表，存储视图数据
		if err != nil {
			return nil, err
		} //创建视图管理器表出来
	}
	return viewManager, nil
}

//CreateView 创建一个视图
func (v *ViewManager) CreateView(vname string, vdef string, tx *tx.Transaction) error {
	layout, err := v.tblgr.GetLayout("viewcat", tx) //获得当前视图表的元数据
	if err != nil {
		return err
	}
	//对这张视图表进行处理
	ts, err := rm.NewTableScan(tx, vname, layout)
	if err != nil {
		return err
	}

	defer ts.Close()
	//向当前的表中插入一条数据
	ts.Insert() //获得一个可用的槽位
	ts.SetString("viewname", vname)
	ts.SetString("viewdef", vdef)
	return nil
}

//GetViewDef 获得某个视图创建时的sql语句
func (v *ViewManager) GetViewDef(vname string, tx *tx.Transaction) (string, error) {
	result := ""
	layout, err := v.tblgr.GetLayout("viewcat", tx) //获得当前视图表的元数据
	if err != nil {
		return "", err
	}
	//对这张视图表进行处理
	ts, err := rm.NewTableScan(tx, vname, layout)
	if err != nil {
		return "", err
	}

	defer ts.Close()
	for ts.Next() {
		//遍历这张表
		if ts.GetString("viewname") == vname {
			//取出他的def出来
			result = ts.GetString("viewdef")
			break
		}
	}
	return result, nil
}
