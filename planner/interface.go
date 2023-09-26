package planner

import rm "miniSQL/record_manager"

/*
	对于给定的sql语句，sql解释器会构建不同的查询树，因此我们需要专门计算哪种的查询树具有最有的效率
	在db种，就是通过Plan模块来实现
*/

type Plan interface {
	Open() (interface{}, error)        //返回Scan 或UpdateScan对象
	BlockAccessed() int                //B(s),放回当前操作会访问到的块数
	RecordsOutput() int                //R(s)返回当前操作会访问的记录数
	DistinctValues(fldName string) int //V(s,F)
	Schema() rm.SchemaInterface        //获得表结构
}
