package metadata_manager

import (
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
)

//IndexInfo 当前索引对应的一系列的信息
type IndexInfo struct {
	indexName   string //索引名字
	fieldName   string //被创建索引的字段
	tableSchema *rm.Schema
	tx          *tx.Transaction
	indexLayout *rm.Layout
	si          *StatInfo
}

func (i *IndexInfo) CreateIndexLayout() *rm.Layout {
	sch := rm.NewSchema() //创建一个新的表结构
	//这个表中有三个字段，block id dataval
	sch.AddIntField("block") //这条记录所在的区块号
	sch.AddIntField("id")    //id就是这条记录在这个区块里面的偏移,在这个block中的第几条记录
	if i.tableSchema.Type(i.fieldName) == rm.INTEGER {
		//如果被创建索引的字段是int类型，就添加一个int字段
		//查找的对应的dataval，如果相同，就把block+id取出来，知道记录在磁盘中的位置
		sch.AddIntField("dataval") //dataval就是当前所查询的索引字段的取值
	} else {
		fldlen := i.tableSchema.Length(i.fieldName) //当前被创建索引的字段的字段的长度
		sch.AddStringField("dataval", fldlen)
	}
	return rm.NewLayoutWithSchema(sch)
}

func NewIndexInfo(indexName string, fieldName string, tableSchema *rm.Schema, tx *tx.Transaction, si *StatInfo) *IndexInfo {

	indexInfo := &IndexInfo{
		indexName:   indexName,
		fieldName:   fieldName,
		tx:          tx,
		tableSchema: tableSchema,
		si:          si,
		indexLayout: nil,
	}
	indexInfo.indexLayout = indexInfo.CreateIndexLayout()
	return indexInfo

}

//BlockAccessed 当前会访问几个block块
func (i *IndexInfo) BlockAccessed() int {
	rpb := int(i.tx.BlockSize()) / i.indexLayout.SlotSize() //计算一个block中有多少条记录
	numBlock := i.si.RecordsOutput() / rpb                  //得到当前使用了多少个block

	return HashIndexSearchCost(numBlock, rpb)
}

//DistinctValue 得知当前的字段中有多少条唯一的记录
func (i *IndexInfo) DistinctValue(fldName string) int {
	if i.fieldName == fldName {
		//如果被构建索引的字段和当前的字段相同，就返回当前的字段的唯一值
		return 1
	}
	return i.si.DistinctValue(fldName)
}

func (i *IndexInfo) Open() Index {
	//在这里构建不同的hash算法对象
	return NewHashIndex(i.tx, i.indexName, i.indexLayout)
}
