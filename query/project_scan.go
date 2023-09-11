package query

import (
	"errors"
	"miniSQL/comm"
)

//ProjectScan 组件：从所有row中把需要的列取出来
type ProjectScan struct {
	scan      Scan
	fieldList []string //当前这个projectScan要从表中提取出来的字段
}

func NewProductionScan(s Scan, fieldList []string) *ProjectScan {
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
func (p *ProjectScan) GetInt(fieldName string) (int, error) {
	//保证当前字段在当前的表中能够得到
	if p.scan.HashField(fieldName) {
		return p.scan.GetInt(fieldName), nil
	}
	return 0, errors.New("Field Not Found")
}

func (p *ProjectScan) GetString(fieldName string) (string, error) {
	//保证当前字段在当前的表中能够得到
	if p.scan.HashField(fieldName) {
		return p.scan.GetString(fieldName), nil
	}
	return "", errors.New("Field Not Found")
}

func (p *ProjectScan) GetVal(fieldName string) (*comm.Constant, error) {
	if p.scan.HashField(fieldName) {
		return p.scan.GetVal(fieldName), nil
	}
	return nil, errors.New("Field Not Found")
}

func (p *ProjectScan) HashField(fieldName string) bool {
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
