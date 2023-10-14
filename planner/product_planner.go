package planner

import (
	"miniSQL/query"
	rm "miniSQL/record_manager"
)

//ProductPlan 查询得到成本最低的连表顺序，当前就先使用两个表的情况 TODO 使用动态规划，处理多表查询的连接顺序的处理
type ProductPlan struct {
	planOrders []Plan
	schema     *rm.Schema //多张表的笛卡尔集，字段是两张表一共的
	cost       float64    //多表查询的成本开销
}

func NewProductPlan(p1 Plan, p2 Plan) *ProductPlan {
	productPlan := &ProductPlan{
		schema: rm.NewSchema(),
	}
	productPlan.schema.AddAll(p1.Schema()) //将所有表中的字段合并，构成新表的schema
	productPlan.schema.AddAll(p2.Schema()) //将所有表中的字段合并，构成新表的schema
	//在初始化的时候，就需要计算出最佳的连表顺序
	//TODO 使用动态规划得到最优的排列顺序
	planOrders, cost := productPlan.FindOptimalJoinOrder(p1, p2)
	productPlan.planOrders = planOrders
	for _, p := range planOrders {
		productPlan.cost += p.Cost()
	}
	productPlan.cost += cost
	return productPlan
}

//FindOptimalJoinOrder 查询得到最佳的连表顺序,以及当前连表顺序得到的成本
func (p *ProductPlan) FindOptimalJoinOrder(p1 Plan, p2 Plan) ([]Plan, float64) {
	p1_p2Cost := float64(p1.BlockAccessed()) + float64(p1.RecordsOutput()*p2.BlockAccessed())*cpuCost + float64(p1.BlockAccessed()*p2.BlockAccessed())*ioCost //计算得到不同成本的开销
	p2_p1Cost := float64(p2.BlockAccessed()) + float64(p2.RecordsOutput()*p2.BlockAccessed())*cpuCost + float64(p1.BlockAccessed()*p2.BlockAccessed())*ioCost
	var planOrders []Plan

	if p1_p2Cost <= p2_p1Cost {
		//当前需要以p1开始
		planOrders = append(planOrders, p1)
		planOrders = append(planOrders, p2)
		return planOrders, p1_p2Cost
	} else {
		//当前需要以p2开始
		planOrders = append(planOrders, p2)
		planOrders = append(planOrders, p1)
		return planOrders, p2_p1Cost
	}
}

//Open 获得当前的productScan
func (p *ProductPlan) Open() (interface{}, error) {
	var (
		s1  interface{}
		s2  interface{}
		err error
	)

	if s1, err = p.planOrders[0].Open(); err != nil {
		return nil, err
	}
	if s2, err = p.planOrders[1].Open(); err != nil {
		return nil, err
	}
	return query.NewProductScan(s1.(query.Scan), s2.(query.Scan)), nil
}

//BlockAccessed 需要先得到成本最低的开销的连表顺序
func (p *ProductPlan) BlockAccessed() int {
	//如果以p1为主，就是p1的访问不变，p2根据p1的记录每次扫描一次表
	return p.planOrders[0].BlockAccessed() + (p.planOrders[0].RecordsOutput() * p.planOrders[1].BlockAccessed())
}

func (p *ProductPlan) DistinctValues(fldName string) int {
	if p.planOrders[0].Schema().HashField(fldName) {
		//如果这个字段在p1种，就返回这个字段在的位置
		return p.planOrders[0].DistinctValues(fldName)
	} else {
		//否则就返回他在p2种的字段数量
		return p.planOrders[1].DistinctValues(fldName)
	}
}

//RecordsOutput 连表查询的总记录访问数量是一定的,当前有多少个记录就返回多少条记录
func (p *ProductPlan) RecordsOutput() int {
	return p.planOrders[0].RecordsOutput() * p.planOrders[1].RecordsOutput()
}

func (p *ProductPlan) Schema() rm.SchemaInterface {
	return p.schema
}

//Cost 需要先通过获得他的最佳连表顺序，从而计算该最小成本之下的成本开销
func (p *ProductPlan) Cost() float64 {
	//初始化完了之后就得到了当前的一个开销了
	return p.cost
}
