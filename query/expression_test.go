package query

import (
	"github.com/stretchr/testify/assert"
	"miniSQL/comm"
	rm "miniSQL/record_manager"
	"testing"
)

func TestExpression(t *testing.T) {
	// 创建一个常量表达式
	ival := 42
	constantVal := comm.NewConstantInt(&ival)
	expressionConstant := NewExpressionWithConstant(constantVal)

	// 测试 IsFieldName 方法
	assert.False(t, expressionConstant.IsFieldName()) //判断当前表达式是否为一个字段
	// 测试 AsFieldName 方法
	assert.Equal(t, "", expressionConstant.AsFieldName())
	// 测试 AsConstant 方法
	assert.Equal(t, constantVal, expressionConstant.AsConstant())
	//
	// 测试 ToString 方法
	assert.Equal(t, "42", expressionConstant.ToString())

	assert.False(t, expressionConstant.IsFieldName())

	fval := "age"
	expressionFld := NewExpressionWithFieldName(fval)
	assert.True(t, expressionFld.IsFieldName())
	assert.Equal(t, fval, expressionFld.ToString())
	assert.Equal(t, fval, expressionFld.AsFieldName())

	sch := rm.NewSchema()
	sch.AddStringField("age", 9)
	assert.True(t, expressionFld.AppliesTo(sch))
	assert.True(t, expressionConstant.AppliesTo(sch))

}
