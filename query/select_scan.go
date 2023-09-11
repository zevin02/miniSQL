package query

/*
	select name from student where age>20,把所有age>20的记录都取出来，但是我们只要其中的name字段
	还有一个scan对象会在select_scan的基础上，把所有的name的给筛选出来
*/
//
//type SelectScan struct {
//	scan UpdateScan //扫描
//	pred *Predicate //select的筛选条件，
//}
//
//func NewSelectScan(s UpdateScan, pred *Predicate) *SelectScan {
//	return &SelectScan{
//		scan: s,
//		pred: pre,
//	}
//}
//
//func (s *SelectScan) BeforeFirst() {
//	s.scan.BeforeFirst()
//}
//
//func (s *SelectScan) Next() bool {
//	for s.scan.Next() {
//		if s.pred.IsSatisfied(s) {
//			//筛选条件满足了，就可以返回
//			return true
//		}
//	}
//	return false
//}
//
//func (s *SelectScan) GetInt(fieldName string) int {
//	return s.scan.GetInt(fieldName)
//}
//
//func (s *SelectScan) GetString(fieldName string) string {
//	return s.scan.GetString(fieldName)
//}
//
//func (s *SelectScan) GetVal(fieldName string) *Constant {
//	return s.scan.GetVal(fieldName)
//}
//
////HashField 判断某个字段是否存在这个表中
//func (s *SelectScan) HashField(fieldName string) bool {
//	return s.scan.HashField(fieldName)
//}
//
//func (s *SelectScan) Close() {
//	s.scan.Close()
//}
//
