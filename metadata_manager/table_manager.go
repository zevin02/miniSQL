package metadata_manager

import (
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
)

//创建数据库表，将表对应的schema和layout存储在数据库表中，或者从数据库表中把这两个数据结构取出,用于创建表的记录

//有两个特殊的数据库表名字是tblcat（tableName string,slotSize int）存储的是表名和一条记录的长度,表的元数据
//fblcat（tableName string,fieldName string,type FIELD_TYPE length,offset）,记录的元数据

const (
	MAX_NAME = 16 //tableName的最大长度
)

type TableManager struct {
	tcatLayout *rm.Layout //存储当前表的元数据
	fcatLayout *rm.Layout //存储当前表每个字段的元数据

}

func NewTableManager(isNew bool, tx *tx.Transaction) *TableManager {
	//这个对象是一个单例,第一次进来才要创建这两张表
	tbMgr := &TableManager{}
	tcatSchema := rm.NewSchema()
	tcatSchema.AddStringField("tblname", MAX_NAME) //当前表添加一个表名字段

	tcatSchema.AddIntField("slotsize")                    //当前表添加一个当前记录的大小
	tbMgr.tcatLayout = rm.NewLayoutWithSchema(tcatSchema) //根据当前的schema创建记录的结构
	fcatSchema := rm.NewSchema()
	fcatSchema.AddStringField("tblname", MAX_NAME) //设置表名
	fcatSchema.AddStringField("fldname", MAX_NAME) //设置字段名
	fcatSchema.AddIntField("type")                 //添加他的类型
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	tbMgr.fcatLayout = rm.NewLayoutWithSchema(fcatSchema)
	if isNew {
		//创建两张表
		tbMgr.CreateTable("tblcat", tcatSchema, tx) //创建一张表,在本地文件中创建一个区块数据，往这个区块写入特定的数据,所以需要使用事务，失败也可以进行回滚操作
		tbMgr.CreateTable("fldcat", fcatSchema, tx) //创建一张表,在本地文件中创建一个区块数据，往这个区块写入特定的数据

	}
	return tbMgr
}

//CreateTable 在特定的磁盘文件中构造这个表的记录
func (t *TableManager) CreateTable(tblName string, schema *rm.Schema, tx *tx.Transaction) {
	layout := rm.NewLayoutWithSchema(schema)
	tcat := rm.newTableScan(tx, "tblcat", t.tcatLayout) //获得表对应的记录描述
	tcat.Insert()
	tcat.SetString("tblname", tblName)                  //写入这个的表名
	tcat.SetString("slotsize", layout.SlotSize())       //写入这个记录的大小
	fcat := rm.NewTableScan(tx, "fldcat", t.fcatLayout) //这个是对这个表的元数据进行管理
	for _, fieldName := range schema.Fields() {
		fcat.Insert()
		//把表的每个记录都添加到表中
		fcat.AddString("tblname", tblName)
		fcat.AddString("fldname", fieldName)
		fcat.AddInt("type", schema.Type(fieldName)) //从schema中获得某个fiedld类型
		fcat.AddInt("length", int(schema.Length(fieldName)))
		fcat.AddInt("offset", schema.Offset(fieldName))
	}
	//以后我们就需要从这两个表中，将特定的记录拿出来
}

//GetLayout 获得给定表的Layout结构
func (t *TableManager) GetLayout(tblName string, tx *tx.Transaction) *rm.Layout {
	size := -1
	//从表中把信息读取出来，再新生成layout结构出来
	tcat := rm.newTableScan(tx, "tblcat", t.tcatLayout) //获得表对应的记录描述
	//遍历这个tblcat表，找到报行这个名称的记录
	for tcat.Next() {
		if tcat.GetString("tblname") == tblName {
			//如果表名就是我们需要的
			size = tcat.GetInt("slotsize") //获得相应表的一条记录的大小
		}
	}
	tcat.Close()
	//访问第二张表
	sch := rm.NewSchema()                               //这个管理这个表信息
	offsets := make(map[string]int)                     //记录每个字段在表中的偏移
	fcat := rm.NewTableScan(tx, "fldcat", t.fcatLayout) //从这个表中获得这个表的信息
	for fcat.Next() {
		if fcat.GetString("tblname") == tblName {
			//这样就得到了这个表的所有信息
			fldName := fcat.GetString("fldname")
			ftype := fcat.GetInt("type")
			length := fcat.GetInt("length")
			offset := fcat.GetInt("offset")
			//写入到map中去
			offsets[fldName] = offset
			sch.AddField(fldName, rm.FIELD_TYPE(ftype), length) //完成当前表的schema的创建
		}
	}
	fcat.Close()
	return rm.NewLayout(sch, offsets, size) //返回字段的相关星系

}
