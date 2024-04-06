package metadata_manager

import (
	"fmt"
	"miniSQL/comm"
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
)

const (
	NUM_BUCKETS = 100 //有100个hash的bucket，就是有100个哈希信息的存储表
)

//HashIndex 这个类实现index这个接口
type HashIndex struct {
	tx        *tx.Transaction
	indexName string //当前索引的名称
	layout    *rm.Layout
	searchKey *comm.Constant
	ts        *rm.TableScan //因为一个字段中可能分散在不同的表中，索引在扫描的时候，哈希冲突，同一个表中的映射到的哈希值都是一样的，所以只需要遍历这个表就行了
}

//NewHashIndex 构建一个哈希索引对象
func NewHashIndex(tx *tx.Transaction, indexName string, layout *rm.Layout) *HashIndex {
	return &HashIndex{
		tx:        tx,
		indexName: indexName,
		layout:    layout,
		ts:        nil,
	}
}

//BeforeFirst 找到我们要处理的那个索引字段名对应的表，并且从他的第一个记录开始进行读取
func (h *HashIndex) BeforeFirst(searchKey *comm.Constant) {
	h.Close()
	h.searchKey = searchKey
	bucket := h.searchKey.HashCode() % NUM_BUCKETS //获得他所属的bucket编号
	//构造索引记录对应的表名称
	tblName := fmt.Sprintf("%s#%d", h.indexName, bucket)
	h.ts, _ = rm.NewTableScan(h.tx, tblName, h.layout) //得到这个表的扫描器

}

//Close 将当前安的泽和该hash索引进行关闭
func (h *HashIndex) Close() {

	if h.ts != nil {
		h.ts.Close()
	}
}

func (h *HashIndex) Next() bool {
	for h.ts.Next() {
		//因为当前表中对里面的字段哈希之后，他的哈希值都是一样的，所以遍历这个表，查询他的dataval即可
		if h.ts.GetVal("dataval").Equal(h.searchKey) {
			//在这个表中查询到这个记录就行了
			return true
		}
	}
	return false
}

//GetDataRID 得到当前要查询的数值存储在哪个区块中
func (h *HashIndex) GetDataRID() *rm.RID {
	blkNum := h.ts.GetInt("block") //因为当前的ts中已经到了那条记录所在的位置，我们就可以取出他在区块中的哪个位置
	id := h.ts.GetInt("id")        //从当前记录中取出他的偏移量
	return rm.NewRID(blkNum, id)
}

//Insert 插入一条记录
func (h *HashIndex) Insert(val *comm.Constant, rid *rm.RID) {
	h.BeforeFirst(val) //这个地方就得到了他需要对哪个表进行一个读写
	h.ts.Insert()
	h.ts.SetInt("block", rid.BlockNumber())
	h.ts.SetInt("id", rid.Slot())
	h.ts.SetVal("dataval", val) //将val进行一个写入
}

//Delete 删除那条索引记录
func (h *HashIndex) Delete(val *comm.Constant, rid *rm.RID) {
	h.BeforeFirst(val) //得到这个记录对应的表
	//遍历这个表，然后将那条记录进行一个删除
	for h.ts.Next() {
		//对这个进行遍历，如果得到的那条记录和我们要删除的记录相同，就将这条记录删除
		if h.GetDataRID().Equals(rid) {
			h.ts.Delete()
			return
		}
	}
}
func HashIndexSearchCost(numblocks int, rpb int) int {
	//得到他这个索引的搜索代价
	return numblocks / NUM_BUCKETS
}
