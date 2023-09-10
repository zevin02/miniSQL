package comm

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConstant(t *testing.T) {
	// 创建一个 int 类型的 Constant
	intVal := 42
	cInt := NewConstantInt(&intVal)

	// 测试 ToString 方法是否正确转换为字符串
	expectedIntStr := "42"
	assert.Equal(t, expectedIntStr, cInt.ToString())

	// 测试 AsInt 方法是否返回正确的 int 值
	assert.Equal(t, intVal, cInt.AsInt())

	// 创建一个 string 类型的 Constant
	strVal := "hello"
	cStr := NewConstantString(&strVal)

	// 测试 ToString 方法是否正确返回字符串值
	assert.Equal(t, strVal, cStr.ToString())
	assert.Equal(t, strVal, cStr.AsString())
	// 测试 AsString 方法是否返回正确的 string 值

	// 创建另一个 int 类型的 Constant
	anotherIntVal := 42
	cAnotherInt := NewConstantInt(&anotherIntVal)

	// 创建一个不同值的 Constant
	diffVal := "different"
	cDiff := NewConstantString(&diffVal)

	// 测试 Equal 方法是否能正确判断两个 Constant 是否相同

	assert.True(t, cInt.Equal(cAnotherInt))
	assert.False(t, cStr.Equal(cAnotherInt))
	assert.False(t, cStr.Equal(cDiff))
	assert.False(t, cInt.Equal(cStr))

}
