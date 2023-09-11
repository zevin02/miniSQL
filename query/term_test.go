package query

import (
	"github.com/stretchr/testify/assert"
	"miniSQL/comm"
	rm "miniSQL/record_manager"
	"testing"
)

func TestTerm(t *testing.T) {
	// 创建两个 Expression 对象，用于构建 Term
	fieldName1 := "MajorId"
	fieldName2 := "DId"
	expression1 := NewExpressionWithFieldName(fieldName1)
	expression2 := NewExpressionWithFieldName(fieldName2)
	// 创建一个 Term 对象
	term := NewTerm(expression1, expression2)

	sch := rm.NewSchema()
	// 测试 IsSatisfied 方法
	assert.False(t, term.AppliesTo(sch))
	// 测试 ToString 方法
	sch.AddStringField("DId", 9)
	sch.AddStringField("MajorId", 9)
	//两个字段都在表中存在，可以进行比较
	assert.True(t, term.AppliesTo(sch))
	expectedString := fieldName1 + "=" + fieldName2
	assert.Equal(t, expectedString, term.ToString())
	// 测试 EquatesWithField 方法
	fieldName := "DId"
	assert.Equal(t, fieldName1, term.EquatesWithField(fieldName))
	// 测试 EquatesWithConstant 方法
	val := 42
	constantVal := comm.NewConstantInt(&val)
	expression3 := NewExpressionWithConstant(constantVal)
	term2 := NewTerm(expression1, expression3)
	assert.True(t, term2.AppliesTo(sch))

}
