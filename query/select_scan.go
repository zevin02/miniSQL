package query

import "miniSQL/comm"

/*
	select name from student where age>20,把所有age>20的记录都取出来，但是我们只要其中的name字段
	还有一个scan对象会在select_scan的基础上，把所有的name的给筛选出来
	关系代数中的SELECT操作是把所有符合条件的行全部抽取出来，而不抽取相应的列
*/

//SelectScan 包含相应的要扫描的表和筛选条件
type SelectScan struct {
	scan UpdateScan //扫描表，同时可以对当前表进行CRUD
	pred *Predicate //select的筛选条件
}

//NewSelectScan 构造一个SelectScan对象
func NewSelectScan(s UpdateScan, pred *Predicate) *SelectScan {
	return &SelectScan{
		scan: s,
		pred: pred,
	}
}

func (s *SelectScan) BeforeFirst() {
	s.scan.BeforeFirst()
}

//Next 跳转到满足条件的下一条记录处
func (s *SelectScan) Next() bool {
	for s.scan.Next() { //如果不满足条件，就继续往后找，找到满足条件的，就返回
		if s.pred.IsSatisfied(s) {
			//筛选条件满足了，就可以返回
			return true
		}
	}
	return false
}
func (s *SelectScan) Insert() {
	s.scan.Insert()
}

func (s *SelectScan) Delete() {
	s.scan.Delete()
}

func (s *SelectScan) GetInt(fieldName string) int {
	return s.scan.GetInt(fieldName)
}
func (s *SelectScan) SetInt(fieldName string, val int) {
	s.scan.SetInt(fieldName, val)
}

func (s *SelectScan) GetString(fieldName string) string {
	return s.scan.GetString(fieldName)
}

func (s *SelectScan) SetString(fieldName string, val string) {
	s.scan.SetString(fieldName, val)
}

func (s *SelectScan) GetVal(fieldName string) *comm.Constant {
	return s.scan.GetVal(fieldName)
}

func (s *SelectScan) SetVal(fieldName string, val *comm.Constant) {
	s.scan.SetVal(fieldName, val)
}

//HasField 判断某个字段是否存在这个表中
func (s *SelectScan) HasField(fieldName string) bool {
	return s.scan.HasField(fieldName)
}

func (s *SelectScan) Close() {
	s.scan.Close()
}
