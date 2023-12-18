package query

import (
	"miniSQL/comm"
	rm "miniSQL/record_manager"
)

//这个文件只是为了对UpdateScan接口进行处理

type UpdateScanWrapper struct {
	scan Scan
}

//NewUpdateScanWrapper 构造一个UpdateScanWrapper对象
func NewUpdateScanWrapper(s Scan) *UpdateScanWrapper {
	return &UpdateScanWrapper{
		scan: s,
	}
}

//GetScan 获得里面的Scan的对象类型
func (u *UpdateScanWrapper) GetScan() Scan {
	return u.scan
}

func (u *UpdateScanWrapper) SetInt(fieldName string, val int) {
	//DO NOTHING
}
func (u *UpdateScanWrapper) SetString(fieldName string, val string) {
	//DO NOTHING
}

func (u *UpdateScanWrapper) SetVal(fieldName string, val *comm.Constant) {
	//DO NOTHING
}

func (u *UpdateScanWrapper) Insert() {
	//DO NOTHING
}

func (u *UpdateScanWrapper) Delete() {
	//DO NOTHING
}

func (u *UpdateScanWrapper) GetRid() rm.RIDInterface {
	return nil
}

func (u *UpdateScanWrapper) Move2Rid(rid rm.RIDInterface) {
	//DO NOTHING
}

func (u *UpdateScanWrapper) BeforeFirst() {
	u.scan.BeforeFirst()
}

func (u *UpdateScanWrapper) Next() bool {
	return u.scan.Next()
}

func (u *UpdateScanWrapper) GetInt(fieldName string) int {
	return u.scan.GetInt(fieldName)
}

func (u *UpdateScanWrapper) GetString(fieldName string) string {
	return u.scan.GetString(fieldName)
}

func (u *UpdateScanWrapper) GetVal(fieldName string) *comm.Constant {
	return u.scan.GetVal(fieldName)

}

func (u *UpdateScanWrapper) HasField(fieldName string) bool {
	return u.scan.HasField(fieldName)

}

func (u *UpdateScanWrapper) Close() {
	u.scan.Close()
}
