package query

import (
	"miniSQL/comm"
	rm "miniSQL/record_manager"
)

//SName = 'joe' and MajorId = DId,这一整个是一个predicate
//MOD(GradYear,4)==0这个式子用term表示,表达式
//MOD(GradYear,4)==0,由=好将其分成了左右两部分。我们给这些部分用expression来表示
//对于MOD(GradYear,4),我们分成了MOD，GradYearpackage query
//其中MOD指定了一个操作operation，GradYear是一个字段名用fieldname表示，4是一个常量，用constant表示

type Expression struct {
	//age >20，age是一个字段，20是一个常量
	val     *comm.Constant //常量
	fldName string         //字段
}

//NewExpressionWithConstant 用一个val来初始化一个expression
func NewExpressionWithConstant(val *comm.Constant) *Expression {
	return &Expression{
		val:     val,
		fldName: "",
	}
}

//NewExpressionWithFieldName 存放入的就是一个字段名
func NewExpressionWithFieldName(fldName string) *Expression {
	return &Expression{
		val:     nil,
		fldName: fldName,
	}
}

//IsFieldName 当前表达式是否是fieldName
func (e *Expression) IsFieldName() bool {
	return e.fldName != ""
}

func (e *Expression) AsFieldName() string {
	return e.fldName
}

func (e *Expression) AsConstant() *comm.Constant {
	return e.val
}

//Evaluate 如果当前是一个常量直接返回这个值，如果当前是一个字段，就需要根据这个字段查询这个值
func (e *Expression) Evaluate(s Scan) *comm.Constant {
	//expression可能是常量也可能是一个字段，如果是后者，我们需要查询这个字段对应的具体值
	if e.val != nil {
		//如果是常量，直接返回这个常量
		return e.val
	}
	//如果当前是字段，就需要查找这个字段对应的值
	return s.GetVal(e.fldName)
}

//AppliesTo 判断当前字段是否可以运用在该表中
func (e *Expression) AppliesTo(sch *rm.Schema) bool {
	//如果是一个常量的话，可以作为判断条件直接用，如果当前表没有某个字段的话，就无法使用
	if e.val != nil {
		return true
	}
	return sch.HashField(e.fldName)
}

//ToString 将当前的常量或者是字段，都按照字符串的形式来表示
func (e *Expression) ToString() string {
	if e.val != nil {
		return e.val.ToString()
	}
	return e.fldName
}
