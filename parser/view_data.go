package parser

import "fmt"

type ViewData struct {
	viewName  string
	queryData *QueryData
}

func NewViewData(viewName string, queryData *QueryData) *ViewData {
	return &ViewData{
		viewName:  viewName,
		queryData: queryData,
	}
}

func (v *ViewData) ViewName() string {
	return v.viewName
}

//ViewDef 获得创建这张表的定义语句
func (v *ViewData) ViewDef() string {
	return v.queryData.ToString()
}

func (v *ViewData) ToString() string {
	s := fmt.Sprintf("view name : %s,view def :%s\n", v.viewName, v.ViewDef())
	return s
}
