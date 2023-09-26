package planner

import rm "miniSQL/record_manager"

type Plan interface {
	Open() (interface{}, error)        //返回Scan 或UpdateScan对象
	BlockAccessed() int                //B(s),放回当前操作会访问到的块数
	RecordsOuput() int                 //R(s)返回当前操作会访问的记录数
	DistinctValues(fldName string) int //V(s,F)
	Schema() rm.SchemaInterface        //获得表结构
}
