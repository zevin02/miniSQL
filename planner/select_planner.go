package planner

import (
	"math"
	"miniSQL/query"
	rm "miniSQL/record_manager"
)

type SelectPlan struct {
	p    Plan             //因为是构建查询树，可能是在tableplan的基础上，也可能是在projectplan的基础上
	pred *query.Predicate //当前select的查询条件
	cost float64          //执行到当前select的开销情况
}

//NewSelectPlan 返回当前的selectPlan对象
func NewSelectPlan(p Plan, pred *query.Predicate) *SelectPlan {
	selectPlan := &SelectPlan{
		p:    p,
		pred: pred,
		cost: p.Cost(), //获得当前之前的所有的成本开销
	}
	//感觉这个地方的成本就不需要加上磁盘IO开销了?
	cost := float64(selectPlan.BlockAccessed())*ioCost + float64(selectPlan.RecordsOutput())*cpuCost //计算出当前的成本
	selectPlan.cost += cost
	return selectPlan
}

//Open 打开当前的selectScan对象
func (s *SelectPlan) Open() (interface{}, error) {
	scan, err := s.p.Open() //打开当前的scan对象，可能是tableScan/projectScan

	if err != nil {
		return nil, err
	}

	//如果调用Scan对象，直接调用其Scan内部对象的接口，如果当前调用的是UpdateScan的接口，那么什么都不用做
	updateScan, ok := scan.(query.UpdateScan) //将当前强制类型转化，看是否能转化成功
	if !ok {
		//不能，说明当前获得的是Scan类型的对象，所以需要将他进行转化成UpdateScan类型的对象
		//传入一个scan类型的变量来初始化updateScanWrapper
		updateScanWrapper := query.NewUpdateScanWrapper(scan.(query.Scan))
		return query.NewSelectScan(updateScanWrapper, s.pred), nil
	}
	//selectScan结构体在初始化的时候，我们需要传入updateScan接口对象，但是很多时候，我们需要传入的是Scan对象，所以我们需要进行转化，如果传入的是Scan的话，我们就需要将他封装成UPdateScan接口对象
	return query.NewSelectScan(updateScan, s.pred), nil //返回当前的selectScan对象
}

//BlockAccessed 当前的SelectPlan的区块访问数量和他底层的Plan相等
func (s *SelectPlan) BlockAccessed() int {
	return s.p.BlockAccessed()
}

/*
	student表 (sname,age)-> 10 records
	age(19,20,21)
	为了简单，我们假设字段不同取值的数量是所有记录的平均
	如果要去统计的话，工作量很麻烦
	where age=19 ->会返回3种
	R(s)=R(st)/V(st,f)
*/
func (s *SelectPlan) RecordsOutput() int {
	return s.p.RecordsOutput() / CalculateReductionFactor(s.pred, s.p)
}

//DistinctValues V(s,F) select的distinct的值需要根据底层的值进行计算
func (s *SelectPlan) DistinctValues(fldName string) int {
	//where A=B 第一种情况，A，B都是字段，第二种情况:A是字段，B是常量
	if s.pred.EquatesWithConstant(fldName) != nil {
		//这个说明是第二种情况，一个是字段，一个是常量,那么查询的结果返回一条数据
		return 1
	} else {
		fldName2 := s.pred.EquatesWithField(fldName)
		//判断是否是第一种情况
		if fldName2 != "" {
			//当前B这个字段是合法的,返回当前的最小值,比如A这个字段有10个值，B这个字段有4个值，那么就只能有4个值
			return int(math.Min(float64(s.p.DistinctValues(fldName)), float64(s.p.DistinctValues(fldName2))))
		} else {
			return s.p.DistinctValues(fldName)
		}
	}
	return 0
}

func (s *SelectPlan) Schema() rm.SchemaInterface {
	return s.p.Schema()
}

func (s *SelectPlan) Cost() float64 {
	//一次磁盘块的IO访问都是1.0的成本开销，一条记录的CPU比对都有0.2的CPU成本开销
	return s.cost
}
