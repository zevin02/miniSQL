package planner

import (
	"miniSQL/query"
	rm "miniSQL/record_manager"
)

//ProjectPlan 对project进行计划
type ProjectPlan struct {
	p      Plan
	schema *rm.Schema //挑选出这几个字段
	cost   float64    //执行到当前project的开销情况
}

//NewProjectPlan 使用传入的需要筛选的字段来构造projectPlan对象
func NewProjectPlan(p Plan, fieldList []string) *ProjectPlan {
	projectPlan := &ProjectPlan{
		p:      p,
		schema: rm.NewSchema(),
		cost:   p.Cost(), //获得当前路径之前的成本开销
	}
	cost := float64(p.BlockAccessed())*ioCost + float64(p.RecordsOutput())*cpuCost //计算出当前的成本
	projectPlan.cost += cost
	for _, field := range fieldList {
		projectPlan.schema.Add(field, projectPlan.p.Schema()) //在他底部表的schema结构的基础上筛选我们需要的字段进来
	}
	return projectPlan
}

func (p *ProjectPlan) Open() (interface{}, error) {
	s, err := p.p.Open() //获得底层的scan对象
	if err != nil {
		return nil, err
	}
	//使用当前获得的子scan对象，和需要的表结构来进行查询
	return query.NewProjectScan(s.(query.Scan), p.schema.Fields()), nil
}

//BlockAccessed 当前project访问的block数量和其子scan是一样的
func (p *ProjectPlan) BlockAccessed() int {
	return p.p.BlockAccessed()
}

//RecordsOutput 当前的project访问的record和子scan是一样的
func (p *ProjectPlan) RecordsOutput() int {
	return p.p.RecordsOutput()
}

//DistinctValues project的distinct取决于底层调用对象的值
func (p *ProjectPlan) DistinctValues(fldName string) int {
	//如果当前字段不存在，也需要访问底层的table
	return p.p.DistinctValues(fldName)

}

func (p *ProjectPlan) Schema() rm.SchemaInterface {
	return p.schema
}

func (p *ProjectPlan) Cost() float64 {
	return p.cost
}
