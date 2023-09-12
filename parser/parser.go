package parser

import (
	"errors"
	"io"
	"miniSQL/comm"
	"miniSQL/lexer"
	"miniSQL/query"
	"strconv"
	"strings"
)

type SQLParser struct {
	sqlLexer *lexer.Lexer //判断当前的组合序列是否符合某种语法规则

}

func NewSQLParser(s string) *SQLParser {
	return &SQLParser{
		sqlLexer: lexer.NewLexer(s),
	}
}

/*
	FIELD -> ID
	CONSTANT -> STRING | NUM
	EXPRESSION -> FIELD | CONSTANT
	TERM -> EXPRESSION EQ EXPRESSION
	PREDICATE -> TERM (AND PREDICATE)?
*/

//Field 解析当前的field，并返回当前的field的token对应的字符串
func (p *SQLParser) Field() (*lexer.Token, string, error) {
	token, err := p.sqlLexer.Scan()
	if err != nil {
		return nil, "", err
	}
	//field的读取进来必须要是一个ID
	if token.Tag != lexer.ID {
		return nil, "", errors.New("tag of field is not ID")
	}

	return token, p.sqlLexer.Lexeme, nil
}

//Constant 当前是一个常数，CONSTANT -> STRING | NUM，返回对应的constant数
func (p *SQLParser) Constant() (*comm.Constant, error) {
	token, err := p.sqlLexer.Scan()
	if err != nil {
		return nil, err
	}
	//判断读取的token的标签，NUM和STRING两种标签
	switch token.Tag {
	case lexer.STRING:
		s := strings.Clone(p.sqlLexer.Lexeme) //把当前的字符串保存起来
		return comm.NewConstantString(&s), nil
		break
	case lexer.NUM:
		v, err := strconv.Atoi(p.sqlLexer.Lexeme) //转化成整数
		if err != nil {
			return nil, errors.New("string is not number")
		}
		return comm.NewConstantInt(&v), nil
		break
	default:
		panic("token is not string ")
	}
	panic("token is not string ")
}

//Expression EXPRESSION -> FIELD | CONSTANT,digui de diaoyong
func (p *SQLParser) Expression() (*query.Expression, error) {
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		return nil, err
	}
	//判断当前的类型，只可能是field和constant两种类型
	if tok.Tag == lexer.ID {
		p.sqlLexer.ReverseScan() //回退
		_, str, err := p.Field() //调用当前的field接口，解析出来field
		if err != nil {
			return nil, err
		}
		return query.NewExpressionWithFieldName(str), nil //使用字符串来初始化当前的表达式
	} else {
		p.sqlLexer.ReverseScan()
		constant, err := p.Constant()
		if err != nil {
			return nil, err
		}
		return query.NewExpressionWithConstant(constant), nil //使用一个常量来初始化当前的表达式
	}
}

//TERM -> EXPRESSION EQ EXPRESSION

func (p *SQLParser) Term() (*query.Term, error) {
	//进行完左边的解析之后
	lhs, err := p.Expression() //先读取左边操作符
	if err != nil {
		return nil, err
	}
	//就需要继续读取到一个=
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		return nil, err
	}
	if tok.Tag != lexer.ASSIGN_OPERATOR {
		return nil, errors.New("should have = in the middle of term")
	}
	rhs, err := p.Expression()
	if err != nil {
		return nil, err
	}
	return query.NewTerm(lhs, rhs), nil
}

//predicate->term (and predicate),条件里面包含条件,递归的调用这个函数
//Predicate构造一个条件出来
func (p *SQLParser) Predicate() *query.Predicate {
	term, err := p.Term()
	if err != nil {
		return nil
	}
	pred := query.NewPredicateWithTerms(term)
	tok, err := p.sqlLexer.Scan() //往后一个op，判断是否是=，如果到最后了，就说明条件读取完了
	if err != nil && tok.Tag != lexer.EOF {
		panic(err)
	}
	//如果当前的是条件是=，就需要递归的调用这个函数，并且把这个predicate进行扩充
	if tok.Tag == lexer.AND {
		pred.ConjoinWith(p.Predicate())
	} else {
		p.sqlLexer.ReverseScan() //如果不是=的话，就需要把操作符放回去,之后还能继续读取
	}
	return pred
}

////query->select selectlist from tablelist (where predicate)
//func (p *SQLParser) Query() *QueryData {
//	//读取当前的关键字
//	tok, err := p.sqlLexer.Scan()
//	if err != nil {
//		return nil
//	}
//	if tok.Tag != lexer.SELECT {
//		panic("token is not select")
//	}
//	//把字段筛选出来
//	fields := p.SelectList()
//	tok, err = p.sqlLexer.Scan()
//	//读取到from关键字
//	if err != nil {
//		return nil
//	}
//	if tok.Tag != lexer.FROM {
//		panic("token is not from")
//	}
//
//	tables := p.TableList()
//	//查看是否有WHERE关键字
//	tok, err = p.sqlLexer.Scan()
//	//读取到from关键字
//	if err != nil {
//		return nil
//	}
//	pred := query.NewPredicate()
//	if tok.Tag == lexer.WHERE {
//		pred = p.Predicate() //当前有where的关键词，就需要获得对应的predicate对象
//	} else {
//		p.sqlLexer.ReverseScan() //把当前读取到的关键字放回去
//	}
//	return newQueryData(fields, tables, pred)
//
//}

//SelectList 把需要的字段筛选出来，递归的读取这个
func (p *SQLParser) SelectList() ([]string, error) {
	//select_list对应select关键字后面的列名称
	l := make([]string, 0)
	_, field, _ := p.Field() //获得一个ID
	l = append(l, field)     //把当前的字段添加到字段列表中

	tok, err := p.sqlLexer.Scan() //往后面读取
	if err != nil {
		panic(err)
	}
	//判断时不是逗号，如果是的话，说明后面还有字段，还需要继续调用
	if tok.Tag == lexer.COMMA {
		selectList, _ := p.SelectList()
		l = append(l, selectList...)
	} else {
		p.sqlLexer.ReverseScan() //把读取到的字符回退，保证下次能够读取到
	}

	return l, nil
}

func (p *SQLParser) TableList() []string {
	l := make([]string, 0)
	_, field, _ := p.Field() //获得一个ID
	l = append(l, field)     //把当前的字段添加到字段列表中
	//如果为空的话，也可以返回了
	tok, err := p.sqlLexer.Scan() //往后面读取
	if err != nil && err != io.EOF {
		panic(err)
	}
	if tok.Tag == lexer.EOF {
		return l
	}
	if tok.Tag == lexer.COMMA {
		tableList := p.TableList()
		l = append(l, tableList...)
	} else {
		p.sqlLexer.ReverseScan() //把读取到的字符回退，保证下次能够读取到
	}
	return l
}
