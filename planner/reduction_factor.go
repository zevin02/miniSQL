package planner

import (
	"math"
	"miniSQL/query"
)

func CalculateReductionFactor(pred *query.Predicate, plan Plan) int {
	factor := 1
	for _, t := range pred.Terms() {
		//在当前的表达式种一个一个的遍历
		factor *= CalculateReductionFactorForTerm(t, plan)
	}
	return factor
}

func CalculateReductionFactorForTerm(t *query.Term, plan Plan) int {
	lhsName := ""
	rhsName := ""
	if t.Lhs().IsFieldName() && t.Rhs().IsFieldName() {
		//如果当前的term的=两边都是字段
		//获得当前的两个字段
		lhsName = t.Lhs().AsFieldName()
		rhsName = t.Rhs().AsFieldName()
		//返回字段最多的值
		if plan.DistinctValues(lhsName) > plan.DistinctValues(rhsName) {
			return plan.DistinctValues(lhsName)
		}
		return plan.DistinctValues(rhsName)
	}
	//其中一个不是字段
	if t.Lhs().IsFieldName() {
		//左边是字段，那么就返回左边的位置值
		lhsName = t.Lhs().AsFieldName()
		return plan.DistinctValues(lhsName)
	}
	if t.Rhs().IsFieldName() {
		//左边是字段，那么就返回左边的位置值
		rhsName = t.Rhs().AsFieldName()
		return plan.DistinctValues(rhsName)
	}
	if t.Lhs().AsConstant().Equal(t.Rhs().AsConstant()) {
		//两个相等的常量
		return 1
	} else {
		return math.MaxInt
	}
}
