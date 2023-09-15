package parser

import "miniSQL/lexer"

/*
	什么叫做语法：给定0-9数字，操作符，+-（）
合法：0,9,（1+2），（1+(2+3)）,
非法：()+1，a，1*2
1+2+（3+4）

1: list-> "("list ")"
2: list-> list+ list
3: list-> list+ list
4: list-> number
5:number->0|1|9
这组规则定义的语言，所有出现在箭头左边的叫做非终结符，所有出现在右边，而没有出现在左边的叫做终结符《无法再被分解》，（+-,0,2..）

4->number ->"4"符合语法条件
 1+2 -->1 ->list  ->number
 a->number! syntax error语法错误
*/

type SimpleParser struct {
	lexer *lexer.Lexer //词法解析器
}

func NewSimpleParser(lexer *lexer.Lexer) *SimpleParser {
	return &SimpleParser{
		lexer: lexer,
	}
}

//1: list-> "("list ")"
//2: list-> list+ list
//3: list-> list+ list
//4: list-> number
//5:number->0|1|9,这些就是推导规则

//把这些规则实现
func (s *SimpleParser) List() error {

}

func (s *SimpleParser) Parse() {

}
