package planner

import rm "miniSQL/record_manager"

const (
	cpuCost float64 = 0.2
	ioCost  float64 = 1.0
)

/*
	对于给定的sql语句，sql解释器会构建不同的查询树，因此我们需要专门计算哪种的查询树具有最有的效率
	在db种，就是通过Plan模块来实现,在构造这个模块的时候，就需要得到他对应的成本开销
*/

//TODO 添加索引后更新对于索引使用的成本计算

type Plan interface {
	Open() (interface{}, error)        //返回Scan 或UpdateScan对象
	BlockAccessed() int                //B(s),放回当前操作会访问到的块数
	RecordsOutput() int                //R(s)返回当前操作会访问的记录数
	DistinctValues(fldName string) int //V(s,F)
	Schema() rm.SchemaInterface        //获得表结构
	Cost() float64                     //当前过程的一个开销
}
