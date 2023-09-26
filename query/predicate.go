package query

import (
	"miniSQL/comm"
	rm "miniSQL/record_manager"
)

/*
	predicate：例如：where (GradYear > 2021 or MOD(GradYear, 4) = 0) and MajorId = DId
	我们发现where后面的语句可以通过or,and关键字分成几个组成部分
	GradYear > 2021,MOD(GradYear, 4)=0,MajorId = DId，这些中的每一个使用都使用term来进行表示,通过将这些term组合起来，就构造除了复杂的predicate对象(语法分析)
	GradYear>2021由一个操作符>,分层gradyear和2021两个部分，这个就可以使用expression来表示
	再分解expression，

	// 创建表达式 SName = 'joe'
	lhs1 := NewExpression("SName")  // 创建左操作数，即属性名 "SName"
	c := Constant("joe")           // 创建常量 "joe"
	rhs1 := NewExpression(c)        // 创建右操作数，即常量 "joe"
	t1 := NewTerm(lhs1, rhs1)       // 创建一个项，将左操作数和右操作数组合起来


	// 创建表达式 MajorId = DId
	lhs2 := NewExpression("MajorId")  // 创建左操作数，即属性名 "MajorId"
	rhs2 := NewExpression("DId")      // 创建右操作数，即属性名 "DId"
	t2 := NewTerm(lhs2, rhs2)         // 创建一个项，将左操作数和右操作数组合起来

	最后，我们使用 ConjoinWith 方法将这两个谓词连接在一起，以表示这两个条件的逻辑 "与" 关系。这样，pred1 和 pred2 将合并成一个新的谓词，表示整个表达式 SName = 'joe' and MajorId = DId。
*/

//Predicate 由一个一个的term组成的逻辑式子，组成在一起的一个条件表达式
type Predicate struct {
	terms []*Term
}

//NewPredicate 构造一个Predicate对象
func NewPredicate() *Predicate {
	return &Predicate{}
}

//NewPredicateWithTerms 使用一个表达式来进行初始化该条件表达式
func NewPredicateWithTerms(term *Term) *Predicate {
	predicate := &Predicate{}
	predicate.terms = make([]*Term, 0)
	predicate.terms = append(predicate.terms, term) //往predicate中添加一个表达式
	return predicate
}

//ConjoinWith 将两个predicate组合在一起
func (p *Predicate) ConjoinWith(pred *Predicate) {
	p.terms = append(p.terms, pred.terms...)
}

//IsSatisfied 判断该条件表达式是否符合条件
func (p *Predicate) IsSatisfied(s Scan) bool {
	//对这个条件表达式中的每个表达式进行判断，是否符合条件
	for _, t := range p.terms {
		if !t.IsSatisfied(s) {
			//如果在整个表达式中有一个不满足，就不成立
			return false
		}
	}
	//所有表达式都满足
	return true
}

//SelectSubPred 从predicate的表达式中筛选出符合当前schema表的表达式集合
func (p *Predicate) SelectSubPred(sch *rm.Schema) *Predicate {
	result := NewPredicate()
	for _, t := range p.terms {
		if t.AppliesTo(sch) {
			//如果该表达式中的两个字段都可以在该表中使用，所以这个表达式可以使用
			result.terms = append(result.terms, t)
		}
	}
	if len(result.terms) == 0 { //如果当前没有一个表达式，说明没有一个表达式可以用
		return nil
	}
	return result

}

//JoinSubPred 从Predicate给定的表达式筛选出既符合sch1,sch2以及两个合并之后的表的式子
func (p *Predicate) JoinSubPred(sch1 *rm.Schema, sch2 *rm.Schema) *Predicate {
	result := NewPredicate()
	newSch := rm.NewSchema() //创建一个新的表结构
	//将给定的两个表全部添加进来
	newSch.AddAll(sch1)
	newSch.AddAll(sch2)
	for _, t := range p.terms {
		//遍历当前term中的所有表达式
		if !t.AppliesTo(sch1) && t.AppliesTo(sch2) && t.AppliesTo(newSch) {
			//如果当前表达式既符合sch1表，又符合sch2表，同时还符合两个表合并之后的表
			result.terms = append(result.terms, t)
		}
	}
	if len(result.terms) == 0 {
		//没有一个满足表结构操作的表达式
		return nil
	}
	return result
}

//EquatesWithConstant 在给定表达式中的字段，返回他相等的常数值
func (p *Predicate) EquatesWithConstant(fieldName string) *comm.Constant {
	for _, t := range p.terms {
		c := t.EquatesWithConstant(fieldName)
		if c != nil {
			return c
		}
	}
	return nil
}

//EquatesWithField 在式子集合中返回和当前的字段相等的字段名
func (p *Predicate) EquatesWithField(fieldName string) string {
	for _, t := range p.terms {
		c := t.EquatesWithField(fieldName)
		if c != "" {
			return c
		}
	}
	return ""
}

//ToString 将当前的表达式按照字符串的形式表示,
func (p *Predicate) ToString() string {
	result := ""
	for i, t := range p.terms {
		if i == 0 {
			result += t.ToString()
		} else {
			//todo 使用相应的逻辑表达式来进行处理
			result += " AND " + t.ToString()
		}
	}
	return result
}

func (p *Predicate) Terms() []*Term {
	return p.terms
}
