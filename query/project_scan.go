package query

import (
	"miniSQL/comm"
)

//ProjectScan 组件：在表中将指定的列全部抽取出来,project和select都支持了scan对象，所以project或者select都可以作为节点访问底层的tablesacn对象，
type ProjectScan struct {
	scan      Scan
	fieldList []string //当前这个projectScan要从表中提取出来的字段
}

func NewProjectScan(s Scan, fieldList []string) *ProjectScan {
	return &ProjectScan{
		scan:      s,
		fieldList: fieldList,
	}
}
func (p *ProjectScan) Next() bool {
	return p.scan.Next()
}

func (p *ProjectScan) BeforeFirst() {
	p.scan.BeforeFirst()
}

//GetInt 取出一个int数,给定的字段必须是用户选择的字段中才可以取出
func (p *ProjectScan) GetInt(fieldName string) int {
	//保证当前字段在当前的表中能够得到
	if p.scan.HasField(fieldName) {
		return p.scan.GetInt(fieldName)
	}
	return 0
}

func (p *ProjectScan) GetString(fieldName string) string {
	//保证当前字段在当前的表中能够得到
	if p.scan.HasField(fieldName) {
		return p.scan.GetString(fieldName)
	}
	return ""
}

func (p *ProjectScan) GetVal(fieldName string) *comm.Constant {
	if p.scan.HasField(fieldName) {
		return p.scan.GetVal(fieldName)
	}
	return nil
}

func (p *ProjectScan) HasField(fieldName string) bool {
	//当当前的查询字段集合中查找，检查fieldname字段是否在字段集合中
	for _, s := range p.fieldList {
		if s == fieldName {
			return true
		}
	}
	return false
}

func (p *ProjectScan) Close() {
	p.scan.Close()
}
