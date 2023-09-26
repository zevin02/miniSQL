package query

import (
	"github.com/stretchr/testify/assert"
	"miniSQL/comm"
	rm "miniSQL/record_manager"
	"testing"
)

func TestPredicate(t *testing.T) {

	fieldName1 := "MajorId"
	fieldName2 := "DId"
	expression1 := NewExpressionWithFieldName(fieldName1)
	expression2 := NewExpressionWithFieldName(fieldName2)
	// 创建一个 Term 对象
	term1 := NewTerm(expression1, expression2)

	p1 := NewPredicateWithTerm(term1)
	fieldName3 := "StuId"
	c1 := 2
	const1 := comm.NewConstantInt(&c1)
	expression3 := NewExpressionWithFieldName(fieldName3)
	expression4 := NewExpressionWithConstant(const1)
	term2 := NewTerm(expression3, expression4)
	p2 := NewPredicateWithTerm(term2)
	p1.ConjoinWith(p2) //将两个表达式结合起来
	assert.Equal(t, "MajorId=DId and StuId=2", p1.ToString())
	assert.Equal(t, "MajorId", p1.EquatesWithField("DId"))
	assert.Equal(t, const1, p1.EquatesWithConstant("StuId"))

	sch := rm.NewSchema()
	sch.AddIntField("age")
	sch.AddIntField("height")
	sch.AddStringField("StuId", 15)
	sch.AddIntField("DId")
	//选择符合sch表结构的表达式
	p3 := p1.SelectSubPred(sch)
	assert.Equal(t, "StuId=2", p3.ToString())
	sch1 := rm.NewSchema()
	//创建第二张表,有一个和第一张表重复的字段，但是不是同一个类型（TODO mysql中不允许出现相同名字的字段）
	sch1.AddIntField("MajorId")
	sch1.AddIntField("StuId")
	sch1.AddIntField("year")
	p4 := p1.JoinSubPred(sch1, sch)
	assert.Nil(t, p4)
}
