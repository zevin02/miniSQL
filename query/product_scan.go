package query

import "miniSQL/comm"

//ProductScan 两张表查询管理,分别遍历两张表，笛卡尔集，多表查询，将两张表中的每一行都和另一表的每一行组合的操作
//inner join
type ProductScan struct {
	scan1 Scan //第一张表的查询
	scan2 Scan //第二张表的查询
}

func NewProductScan(s1 Scan, s2 Scan) *ProductScan {
	p := &ProductScan{
		scan1: s1,
		scan2: s2,
	}
	p.scan1.Next() //进入到第一个有效的slot
	return p
}

//BeforeFirst 将cursor移动到第一个有效的位置上
func (p *ProductScan) BeforeFirst() {
	//从第一张表的有效slot开始遍历
	p.scan1.BeforeFirst()
	p.scan1.Next()
	p.scan2.BeforeFirst()
}

//Next 迭代两张表的笛卡尔集
func (p *ProductScan) Next() bool {
	//保持p.scan1不变，p.scan2不断向后迭代，形成一组集合，当scan2完了之后，scan2回到起点，scan1移动到下一个位置，scan2继续上述的迭代，组成笛卡尔集合
	if p.scan2.Next() {

		return true
	} else {
		//第一张表已经遍历完了
		p.scan2.BeforeFirst()
		return p.scan2.Next() && p.scan1.Next() //
	}
}

func (p *ProductScan) GetInt(fieldName string) int {
	if p.scan1.HashField(fieldName) {
		return p.scan1.GetInt(fieldName)
	} else {
		return p.scan2.GetInt(fieldName)
	}
}

func (p *ProductScan) GetString(fieldName string) string {
	if p.scan1.HashField(fieldName) {
		return p.scan1.GetString(fieldName)
	} else {
		return p.scan2.GetString(fieldName)
	}
}

func (p *ProductScan) GetVal(fieldName string) *comm.Constant {
	if p.scan1.HashField(fieldName) {
		return p.scan1.GetVal(fieldName)
	} else {
		return p.scan2.GetVal(fieldName)
	}
}

//HashField 如果两张表中有一个满足这个表即可
func (p *ProductScan) HashField(fieldName string) bool {
	return p.scan1.HashField(fieldName) || p.scan2.HashField(fieldName)
}

func (p *ProductScan) Close() {
	p.scan1.Close()
	p.scan2.Close()
}
