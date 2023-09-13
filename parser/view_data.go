package parser

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
