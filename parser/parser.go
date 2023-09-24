package parser

import (
	"errors"
	"fmt"
	"io"
	"miniSQL/comm"
	"miniSQL/lexer"
	"miniSQL/query"
	rm "miniSQL/record_manager"
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
	bfd范式
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

//Expression EXPRESSION -> FIELD | CONSTANT
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

//TERM  -> EXPRESSION EQ EXPRESSION

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

//Predicate 构造一个条件出来
func (p *SQLParser) Predicate() (*query.Predicate, error) {
	term, err := p.Term()
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicateWithTerms(term)
	//tok, err := p.sqlLexer.Scan() //往后一个op，判断是否是=，如果到最后了，就说明条件读取完了
	//if err != nil && tok.Tag != lexer.EOF {
	//	panic(err)
	//}
	ok, err := p.isMatchTag(lexer.AND)
	if err != nil {
		return nil, err
	}
	//如果当前的是条件是=，就需要递归的调用这个函数，并且把这个predicate进行扩充
	if ok {
		qp, err := p.Predicate()
		if err != nil {
			if err == io.EOF {
				return pred, nil
			}
			return nil, err
		}
		if qp != nil {
			pred.ConjoinWith(qp)
		}
	} else {
		p.sqlLexer.ReverseScan() //如果不是=的话，就需要把操作符放回去,之后还能继续读取
	}
	return pred, nil
}

//Query ->select selectlist from tablelist (where predicate)解析出sql语句的各个信息
func (p *SQLParser) Query() (*QueryData, error) {
	//读取当前的关键字
	if err := p.checkWordTag(lexer.SELECT); err != nil {
		return nil, err
	}
	//把字段筛选出来
	fields := p.IDList()
	if err := p.checkWordTag(lexer.FROM); err != nil {
		return nil, err

	}
	tables := p.IDList()
	pred := query.NewPredicate()
	//检查是否有WHERE关键字
	ok, err := p.isMatchTag(lexer.WHERE)
	if err != nil {
		return nil, err
	}
	if ok {
		pred, err = p.Predicate() //当前有where的关键词，就需要获得对应的predicate对象
		if err != nil {
			return nil, err
		}
	} else {
		p.sqlLexer.ReverseScan() //把当前读取到的关键字放回去
	}
	return NewQueryData(fields, tables, pred), nil

}

//IDList 将ID全筛选出来
func (p *SQLParser) IDList() []string {
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
		IDList := p.IDList()
		l = append(l, IDList...)
	} else {
		p.sqlLexer.ReverseScan() //把读取到的字符回退，保证下次能够读取到
	}
	return l
}

func (p *SQLParser) CostantList() []*comm.Constant {
	l := make([]*comm.Constant, 0)
	constant, _ := p.Constant() //获得一个ID
	l = append(l, constant)     //把当前的字段添加到字段列表中
	//如果为空的话，也可以返回了
	tok, err := p.sqlLexer.Scan() //往后面读取
	if err != nil && err != io.EOF {
		panic(err)
	}
	if tok.Tag == lexer.EOF {
		return l
	}
	if tok.Tag == lexer.COMMA {
		constList := p.CostantList()
		l = append(l, constList...)
	} else {
		p.sqlLexer.ReverseScan() //把读取到的字符回退，保证下次能够读取到
	}
	return l
}

//UpdateCmd 对于表的修改的语句主要有:INSERT | DELETE | MODIFY | CREATE,除了这几个之外的话，就是语法错误
func (p *SQLParser) UpdateCmd() (interface{}, error) {
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		return nil, ErrSyntax
	}
	p.sqlLexer.ReverseScan() //读取完了之后，先退回去，让该token能够继续被读取

	if tok.Tag == lexer.INSERT {
		return p.Insert()
	} else if tok.Tag == lexer.DELETE {
		return p.Delete()
	} else if tok.Tag == lexer.UPDATE {
		return p.Update()
	} else if tok.Tag == lexer.CREATE {
		//当前是create,进入到create的分支中
		//p.sqlLexer.ReverseScan()
		return p.Create()
	}
	return nil, ErrSyntax
}

func (p *SQLParser) Create() (interface{}, error) {
	if err := p.checkWordTag(lexer.CREATE); err != nil {
		return nil, err
	}
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		return nil, err
	}
	//先读取一个token查看是create table，view,还是index，这个就不需要再吐回去了,如果这几个create语句后面跟着的都不是，那么就是语法错误了
	if tok.Tag == lexer.TABLE {
		return p.CreateTable()
	} else if tok.Tag == lexer.VIEW {
		return p.CreateView()
	} else if tok.Tag == lexer.INDEX {
		return p.CreateIndex()
		//return nil
	}
	return nil, ErrSyntax
}

//Insert insert into ID (name,age) values (10,"str"),(20,“name”)
//insert into ID left_bracket fieldlist right_bracket values left_bracket valuelist right_bracket
func (p *SQLParser) Insert() (interface{}, error) {
	p.checkWordTag(lexer.INSERT)
	p.checkWordTag(lexer.INTO)
	p.checkWordTag(lexer.ID)
	tblName := p.sqlLexer.Lexeme //得到这张表的表名
	p.checkWordTag(lexer.LEFT_BRACKET)
	//得到他的field集合
	fields := p.IDList()
	p.checkWordTag(lexer.RIGHT_BRACKET)
	p.checkWordTag(lexer.VALUES)
	p.checkWordTag(lexer.LEFT_BRACKET)
	values := p.CostantList()

	p.checkWordTag(lexer.RIGHT_BRACKET)
	return NewInsertData(tblName, fields, values), nil
}

//checkWordTag 检查tag是否是我们需要的tag,如果不是就panic
func (p *SQLParser) checkWordTag(wordTag lexer.Tag) error {
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		return err
		//panic(err) //发出一个error回去
	}
	//table后面一定要跟着一个表名,数字是不能作为一个表名
	if tok.Tag != wordTag {
		//第一个token一定要以create开头，否则就是一个语法错误
		return errors.New(fmt.Sprintf("you have an error in your SQL syntax,should be %s", lexer.TokenMap[wordTag])) //返回一个错误是我们需要的
	}
	return nil
}

//isMatchTag 判断后面是否是wordtag，这个字段可以有也可以没有，true说明后面有这个字段，false说明
func (p *SQLParser) isMatchTag(wordTag lexer.Tag) (bool, error) {
	tok, err := p.sqlLexer.Scan()
	if err != nil && err != io.EOF { //如果当前的err是eof
		return false, err
	}
	if err == io.EOF {
		return false, nil
	}
	//table后面一定要跟着一个表名,数字是不能作为一个表名
	if tok.Tag != wordTag {
		//第一个token一定要以create开头，否则就是一个语法错误
		if err != io.EOF {
			return false, ErrSyntax
		}
	}
	//eof说明也是没有这个字段的
	return true, nil
}

//CreateTable create table tblname (f1 int, f2 varchar(255))
func (p *SQLParser) CreateTable() (interface{}, error) {
	if err := p.checkWordTag(lexer.ID); err != nil {
		return nil, err
	}
	tblName := p.sqlLexer.Lexeme //获得当前的表名
	if err := p.checkWordTag(lexer.LEFT_BRACKET); err != nil {
		return nil, err
	}
	//左括号后面跟着的就是类型的定义
	sch := p.FieldDefs() //读取当前表的结构
	//表结构读取完之后，跟着的就是一个右括号，表示类型定义结束
	if err := p.checkWordTag(lexer.RIGHT_BRACKET); err != nil {
		return nil, err
	}
	//存储当前的表名和表结构
	return NewCreateTableData(tblName, sch), nil
}

//FieldDefs 读取sql语句获得表的schema结构，递归的调用该函数进行读取
func (p *SQLParser) FieldDefs() *rm.Schema {
	schema := p.FieldDef()        //读取一个schema对象
	tok, err := p.sqlLexer.Scan() //读取下一个词
	if err != nil {
		panic(err)
	}
	//如果有逗号，说明后面还有定义，集聚需要继续递归的去调用这个函数
	if tok.Tag == lexer.COMMA {
		schema2 := p.FieldDefs()
		schema.AddAll(schema2)
	} else {
		//如果当前读取到的不是逗号，就把读到的数据放回去
		p.sqlLexer.ReverseScan()
	}
	return schema
}

//FieldDef 读取当前表的字段名字和类型
func (p *SQLParser) FieldDef() *rm.Schema {
	_, fldName, err := p.Field() //读取字段名字
	if err != nil {
		return nil
	}

	return p.fieldType(fldName)
}

//fieldType 读取表的类型
func (p *SQLParser) fieldType(fieldName string) *rm.Schema {
	schema := rm.NewSchema()
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	//字段目前只支持两种类型
	if tok.Tag == lexer.INT {
		schema.AddIntField(fieldName)
	} else if tok.Tag == lexer.VARCHAR {
		//如果是varchar的话，后面还有括号varchar(255)
		tok, err = p.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}
		if tok.Tag != lexer.LEFT_BRACKET {
			panic("should be left bracket")
		}
		//继续往后面读，读取的必须是数字
		tok, err = p.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}
		if tok.Tag != lexer.NUM {
			panic("should be NUM")
		}
		num := p.sqlLexer.Lexeme
		fldLen, err := strconv.Atoi(num)
		if err != nil {
			panic(err)
		}
		schema.AddStringField(fieldName, fldLen)
		//再检查后面是否有）结尾
		tok, err = p.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}
		if tok.Tag != lexer.RIGHT_BRACKET {
			panic("should be right bracket")
		}
	}
	return schema
}

//CreateView 创建一个视图,CREATE VIEW VIEW_NAME AS QUERY
func (p *SQLParser) CreateView() (interface{}, error) {
	if err := p.checkWordTag(lexer.ID); err != nil {
		return nil, err
	}
	viewName := p.sqlLexer.Lexeme
	if err := p.checkWordTag(lexer.AS); err != nil {
		return nil, err
	}
	qd, err := p.Query() //前面的都解析完了，后面就是解析获得query的对象
	return NewViewData(viewName, qd), err
}

//CreateIndex  create index_name ON table_name (column1, column2, ...);
//index_name是要创建索引的关键字， tablename是要在哪张表中创建，后面就是包含索引的列
//查询的时候，：SELECT * FROM employees WHERE last_name = 'Smith';使用这个索引来查询

func (p *SQLParser) CreateIndex() (interface{}, error) {
	p.checkWordTag(lexer.ID)
	indexName := p.sqlLexer.Lexeme
	p.checkWordTag(lexer.ON)
	p.checkWordTag(lexer.ID)
	tableName := p.sqlLexer.Lexeme     //当前要操作的表名
	p.checkWordTag(lexer.LEFT_BRACKET) //左括号
	//获得字段的名字
	fields := p.IDList()
	p.checkWordTag(lexer.RIGHT_BRACKET) //左括号
	idxData := NewIndexData(indexName, tableName, fields)
	return idxData, nil
}

//Delete DELETE FROM students WHERE predicate
func (p *SQLParser) Delete() (interface{}, error) {
	if err := p.checkWordTag(lexer.DELETE); err != nil {
		return nil, err
	}

	if err := p.checkWordTag(lexer.FROM); err != nil {
		return nil, err
	}
	if err := p.checkWordTag(lexer.ID); err != nil {
		return nil, err
	}
	tableName := p.sqlLexer.Lexeme
	pred := query.NewPredicate()
	//如果当前匹配是WHERE的话，就需要获得相应的SQL语句
	ok, err := p.isMatchTag(lexer.WHERE)
	if err != nil {
		return nil, err
	}
	if ok {
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewDeleteData(tableName, pred), nil
}

func (p *SQLParser) Update() (interface{}, error) {
	p.checkWordTag(lexer.UPDATE)
	p.checkWordTag(lexer.ID)
	tableName := p.sqlLexer.Lexeme
	p.checkWordTag(lexer.SET)
	_, fldName, _ := p.Field()
	p.checkWordTag(lexer.ASSIGN_OPERATOR)
	newVal, _ := p.Expression()

	pred := query.NewPredicate()
	//如果当前匹配是WHERE的话，就需要获得相应的SQL语句
	ok, err := p.isMatchTag(lexer.WHERE)
	if err != nil {
		return nil, err
	}
	if ok {
		pred, err = p.Predicate()
		if err != nil {
			return nil, err
		}
	}
	return NewUpdateData(tableName, fldName, newVal, pred), nil
}
