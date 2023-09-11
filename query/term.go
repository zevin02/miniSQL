package query

import (
	"miniSQL/comm"
	rm "miniSQL/record_manager"
)

//Term MOD(GradYear,4)==0这个式子用term表示,表达式
//MajorId = DId
type Term struct {
	lhs *Expression //左表达式,对于上面的例子，这个就是MajorId
	rhs *Expression //右表达式,对于上面的例子，这个就是DId
}

func NewTerm(lhs *Expression, rhs *Expression) *Term {
	return &Term{
		lhs: lhs,
		rhs: rhs,
	}
}

//IsSatisfied 如果是字段就查表拿到这个字段的值，如果是常量就直接获得这个值，判读这两个对应的值是否相同,判断是否符合条件
func (t *Term) IsSatisfied(s Scan) bool {
	//evaluate获得的是一个常量对象，所以可以直接比较
	lhsVal := t.lhs.Evaluate(s)
	rhsVal := t.rhs.Evaluate(s)
	return lhsVal.Equal(rhsVal) //判读两个字段是否相同
}

//AppliesTo 判读这两个字段是否可以使用在对于这张表达的操作
func (t *Term) AppliesTo(sch *rm.Schema) bool {
	return t.lhs.AppliesTo(sch) && t.rhs.AppliesTo(sch)
}

//ToString 把这个表达式转化成字符串的形式
//TODO 操作符后续我们需要处理
func (t *Term) ToString() string {
	return t.lhs.ToString() + "=" + t.rhs.ToString()
}

//EquatesWithField 检查是否存在与给定字段相等的另一字段名字
//MajorId = DId”这两个都是字段，给定的fieldName := "DId"，得到MajorId
func (t *Term) EquatesWithField(fieldName string) string {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && t.rhs.IsFieldName() {
		return t.rhs.AsFieldName() //如果左右两边都是字段，同时左边的字段和给定的字段相同，那么我们得到和这个相同的右边字段
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && t.lhs.IsFieldName() {
		return t.lhs.AsFieldName()
	}
	return ""
}

//EquatesWithConstant 查询给定的字段相等的一个常数
//pid=20,fieldName=pid，返回=20
func (t *Term) EquatesWithConstant(fieldName string) *comm.Constant {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.rhs.AsConstant() //左边是字段，右边是一个常量，左边字段和给定的字段相同，我们返回右边的常量
	} else if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && t.lhs.IsFieldName() {
		//右边是字段，左边是一个常量，左边字段和给定的字段相同，我们返回左边的常量
		return t.lhs.AsConstant()
	} else {
		return nil
	}
}
