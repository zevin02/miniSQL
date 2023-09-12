package lexer

import (
	"fmt"
	"strconv"
)

/*
	1.词法分析：当编译器在词法分析的时候扫描源代码，并识别lexeme（单词/符号），包括数字，就会生成一个NUM标记，并将数字的值保存在该标记中
	2.语法分析：NUM标记作为语法规则中的终结符号之一，语法分析器用这些标记构建语法树，来表示原代码的结构
	3.类型检查：编译器后期，使用NUM标记来做类型检查，将他当作整数或浮点数进行处理


*/

//Num 通常作为数字的标记，用来表示数值，包括整数

type Num struct {
	Tag   *Token
	value int //Num包含的值
}

func NewNumToken(val int) *Num {
	return &Num{
		Tag:   NewToken(NUM),
		value: val,
	}
}

//ToString 把这个Num代表的值按照字符串来表示
func (n *Num) ToString() string {
	return strconv.Itoa(n.value)
}

//Real 实数，包含大数和小数
type Real struct {
	Tag   *Token
	value float64
}

func NewRealToken(val float64) *Real {
	return &Real{
		Tag:   NewToken(REAL),
		value: val,
	}
}

func (r *Real) ToString() string {
	return fmt.Sprintf("%.7f", r.value)
}
