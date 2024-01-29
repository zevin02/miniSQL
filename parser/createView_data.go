package parser

import "fmt"

//CreateViewData 创建一个视图
type CreateViewData struct {
	viewName  string
	queryData *QueryData
}

func NewViewData(viewName string, queryData *QueryData) *CreateViewData {
	return &CreateViewData{
		viewName:  viewName,
		queryData: queryData,
	}
}

func (v *CreateViewData) ViewName() string {
	return v.viewName
}

//ViewDef 获得创建这张表的定义语句
func (v *CreateViewData) ViewDef() string {
	return v.queryData.ToString()
}

func (v *CreateViewData) ToString() string {
	s := fmt.Sprintf("view name : %s,view def :%s\n", v.viewName, v.ViewDef())
	return s
}
