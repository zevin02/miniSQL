package metadata_manager

import (
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
)

//IndexManager 索引管理器，索引表里面管理一些字段,对应的一个元数据表名就是idxcat
type IndexManager struct {
	layout  *rm.Layout
	tblMgr  *TableManager
	statMgr *StatManager
}

//NewIndexManager 创建一个索引管理器
func NewIndexManager(isNew bool, tblMgr *TableManager, statMgr *StatManager, tx *tx.Transaction) *IndexManager {
	if isNew {
		//索引元数据表包含三个字段，索引名，对应的表名，被索引的字段名
		sch := rm.NewSchema()
		sch.AddStringField("indexName", MAX_NAME) //给当前表结构中增加一个字段,为索引名
		sch.AddStringField("tableName", MAX_NAME) //给当前表结构中增加一个字段，为当前对应的表名
		sch.AddStringField("fieldName", MAX_NAME) //当前表结构中增加一个字段，为当前被索引的字段名
		tblMgr.CreateTable("idxcat", sch, tx)     //创建一个索引表，来管理所有的索引数据
	}
	layout, _ := tblMgr.GetLayout("idxcat", tx)

	//创建一个索引管理器
	idxMgr := &IndexManager{
		tblMgr:  tblMgr,
		statMgr: statMgr,
		layout:  layout,
	}
	return idxMgr

}

//CreateIndex 创建一个索引,todo 在插入一条索引的时候，需要检查一下当前这条记录是否已经被插入过了
func (i *IndexManager) CreateIndex(indexName string, tableName string, fieldName string, tx *tx.Transaction) {
	//索引创建的时候，就为他在索引元数据表中添加一条记录
	ts, _ := rm.NewTableScan(tx, "idxcat", i.layout) //对当前的索引元数据表进行读取
	ts.BeforeFirst()                                 //从头开始读取
	ts.Insert()                                      //找到第一个可以插入的位置进行插入
	ts.SetString("indexName", indexName)
	ts.SetString("tableName", tableName)
	ts.SetString("fieldName", fieldName)
	ts.Close()

}

//GetIndexInfo 获得某个字段的,我们需要取获得一个索引的时候，首先就需要获得一个IndexInfo对象，由这个对象来决定，创建哪一种索引算法（可能一个字段是hash索引，一个是b+树索引）
func (i *IndexManager) GetIndexInfo(tableName string, tx *tx.Transaction) map[string]*IndexInfo {
	result := make(map[string]*IndexInfo)
	ts, _ := rm.NewTableScan(tx, "idxcat", i.layout) //获得idxcat这个表读取器
	ts.BeforeFirst()                                 //把游标设置在开头
	for ts.Next() {
		//遍历这个表
		if ts.GetString("tableName") == tableName {
			//读取一条记录，如果发现他这条记录对应的表名就是这个表，就是这条记录，单独弄出来
			indexName := ts.GetString("indexName") //从里面读取出来索引名
			fieldName := ts.GetString("fieldName")
			//这个地方的问题就是他把这个区块的buffer给删除掉了
			tblLayout, _ := i.tblMgr.GetLayout(tableName, tx) //从表管理器中得到他的这layout
			tblSi, _ := i.statMgr.GetStatInfo(tableName, tblLayout, tx)

			schema, ok := tblLayout.Schema().(*rm.Schema) //从layout中强制类型转换获得他的schema
			if !ok {
				panic("schema is not a schema")
			}

			ii := NewIndexInfo(indexName, fieldName, schema, tx, tblSi) //构造一个索引信息对象
			//把当前表中的索引有被添加索引的字段都弄出来，进行一个管理访问
			result[fieldName] = ii //某个字段的索引信息,放到map中进行一个管理

		}
	}
	ts.Close() //这个区块不用再占用缓冲区了
	return result

}
