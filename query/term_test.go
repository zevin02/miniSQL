package query

import (
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

	// 创建一个模拟的 Scan 实现
	//mockScan := &MockScan{} // 你需要创建一个 MockScan 实现 Scan 接口

	// 创建一个模拟的 Schema 对象
	mockSchema := &rm.Schema{} // 你需要创建一个模拟的 Schema

	// 测试 IsSatisfied 方法
	// 在这里你可以添加模拟查询逻辑，确保 IsSatisfied 返回了正确的布尔值

	// 测试 AppliesTo 方法
	if !term.AppliesTo(mockSchema) {
		t.Error("AppliesTo() returned false for valid field names")
	}

	// 测试 ToString 方法
	expectedString := fieldName1 + "=" + fieldName2
	if str := term.ToString(); str != expectedString {
		t.Errorf("ToString() returned %s, expected %s", str, expectedString)
	}

	// 测试 EquatesWithField 方法
	fieldName := "DId"
	if result := term.EquatesWithField(fieldName); result != fieldName1 {
		t.Errorf("EquatesWithField() returned %s, expected %s", result, fieldName1)
	}

	// 测试 EquatesWithConstant 方法
	val := 42
	constantVal := comm.NewConstantInt(&val)
	expression3 := NewExpressionWithConstant(constantVal)
	term2 := NewTerm(expression1, expression3)
	if result := term2.EquatesWithConstant(fieldName); result != constantVal {
		t.Errorf("EquatesWithConstant() returned a different constant")
	}
}
