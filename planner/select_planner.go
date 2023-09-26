package planner

import (
	"math"
	"miniSQL/query"
	rm "miniSQL/record_manager"
)

type SelectPlan struct {
	p    Plan             //因为是构建查询树，可能是在tableplan的基础上，也可能是在projectplan的基础上
	pred *query.Predicate //当前select的查询条件
}

//NewSelectPlan 返回当前的selectPlan对象
func NewSelectPlan(p Plan, pred *query.Predicate) *SelectPlan {
	return &SelectPlan{
		p:    p,
		pred: pred,
	}
}

//Open 打开当前的selectScan对象
func (s *SelectPlan) Open() (interface{}, error) {
	scan, err := s.p.Open() //打开当前的scan对象，可能是tableScan/projectScan
	if err != nil {
		return nil, err
	}
	return query.NewSelectScan(scan.(query.UpdateScan), s.pred), nil //返回当前的selectScan对象
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
