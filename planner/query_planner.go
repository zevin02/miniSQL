package planner

import (
	mm "miniSQL/metadata_manager"
	"miniSQL/parser"
	tx "miniSQL/transaction"
)

type BasicQueryPlan struct {
	mdm *mm.MetaDataManager //对应元数据管理器
}

//NewBasicQueryPlan 创建一个基本的查询计划，传入元数据管理器（表管理，视图管理，统计数据管理）
func NewBasicQueryPlan(mdm *mm.MetaDataManager) *BasicQueryPlan {
	return &BasicQueryPlan{
		mdm: mdm,
	}
}

//CreatePlan 创建一个查询计划
func (b *BasicQueryPlan) CreatePlan(data *parser.QueryData, tx *tx.Transaction) Plan {
	//1.直接创建QueryData 对象中的表
	plans := make([]Plan, 0)
	tables := data.Tables() //从查询的抽象语法树中获得对应的表名
	for _, tblname := range tables {
		//遍历当前的表名
		//获得该表对应的视图的SQL语句
		viewdef, err := b.mdm.GetViewDef(tblname, tx) //从视图管理器中查询当前是否有这个表名
		if err != nil {
			return nil
		}
		if viewdef != "" {
			//当前并不存在相应的视图
			//直接创建表对应的视图
			parsers := parser.NewSQLParser(viewdef) //获得这个视图对应的SQL语句
			viewData, err := parsers.Query()        //重新获得它对应的抽象语法树结构
			if err != nil {
				return nil
			}
			//递归的创建对应的表的Planner
			plans = append(plans, b.CreatePlan(viewData, tx))
		} else {
			//管理的视图中并没有当前的这个sql语句
			pl, _ := NewTablePlan(tx, tblname, b.mdm) //构造一个当前的表Plan
			plans = append(plans, pl)
		}
	}
	//将所有的表执行Product（笛卡尔积操作），表的顺序对后续的查询效率有很大的影响，这里我们并没有考虑表的顺序影响
	//只是按照给定的表依次执行Product操作，后续我们会进行优化
	p := plans[0]
	pps := plans[1:]
	//首先先将表执行笛卡尔积操作
	for _, nextPlan := range pps {
		p = NewProductPlan(p, nextPlan) //将所有的表执行Product（笛卡尔积操作）
	}
	//再执行Select算子
	p = NewSelectPlan(p, data.Pred())
	//再执行project投影操作,把指定的字段给筛选出来
	return NewProjectPlan(p, data.Fields())

}
