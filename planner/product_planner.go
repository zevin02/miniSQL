package planner

import (
	"miniSQL/query"
	rm "miniSQL/record_manager"
)

type ProductPlan struct {
	p1     Plan
	p2     Plan
	schema *rm.Schema //两张表的笛卡尔集，字段是两张表一共的
}

func NewProductPlan(p1 Plan, p2 Plan) *ProductPlan {
	productPlan := &ProductPlan{
		p1:     p1,
		p2:     p2,
		schema: rm.NewSchema(),
	}
	productPlan.schema.AddAll(p1.Schema())
	productPlan.schema.AddAll(p2.Schema())
	return productPlan
}

//Open 获得当前的productScan
func (p *ProductPlan) Open() (interface{}, error) {
	var (
		s1  interface{}
		s2  interface{}
		err error
	)
	if s1, err = p.p1.Open(); err != nil {
		return nil, err
	}
	if s2, err = p.p1.Open(); err != nil {
		return nil, err
	}
	return query.NewProductScan(s1.(query.Scan), s2.(query.Scan)), nil

}

func (p *ProductPlan) BlockAccessed() int {
	//如果以p1为主，就是p1的访问不变，p2根据p1的记录每次扫描一次表
	return p.p1.BlockAccessed() + (p.p1.RecordsOuput() * p.p2.BlockAccessed())
}

func (p *ProductPlan) DistinctValues(fldName string) int {
	if p.p1.Schema().HashField(fldName) {
		//如果这个字段在p1种，就返回这个字段在的位置
		return p.p1.DistinctValues(fldName)
	} else {
		//否则就返回他在p2种的字段数量
		return p.p2.DistinctValues(fldName)
	}
}

func (p *ProductPlan) RecordsOuput() int {
	return p.p1.RecordsOuput() * p.p2.RecordsOuput()
}

func (p *ProductPlan) Schema() rm.SchemaInterface {
	return p.schema
}
