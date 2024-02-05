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

//TableManager 表管理器器
type TableManager struct {
	tcatLayout *rm.Layout //存储当前表的元数据，该表存储的是每个表的表名字，和该表对应一条记录的大小
	fcatLayout *rm.Layout //存储每个表每个字段的元数据

}

//NewTableManager isnew=TRUE 表明当前的表管理器是第一次创建的，就需要创建两张元数据表
func NewTableManager(isNew bool, tx *tx.Transaction) (*TableManager, error) {
	//这个对象是一个单例,第一次进来才要创建这两张表
	tbMgr := &TableManager{}
	//创建两张表进行管理所有表的元数据
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
		//当前数据库第一次创建这两张表创建两张表
		//创建一张表,在本地文件中创建一个区块数据，往这个区块写入特定的数据,所以需要使用事务，失败也可以进行回滚操作
		//因为这两个也是表，所以也需要添加到这两个表管理的表中进行管理
		if err := tbMgr.CreateTable("tblcat", tcatSchema, tx); err != nil {
			return nil, err
		}
		if err := tbMgr.CreateTable("fldcat", fcatSchema, tx); err != nil {
			return nil, err
		}
	}
	return tbMgr, nil
}

//CreateTable 创建一张表，并添加到tbcat和fldcat两张表进行管理,在创建表之前首先保证tblcat和fldcat两张元数据表存在
func (t *TableManager) CreateTable(tblName string, schema *rm.Schema, tx *tx.Transaction) error {
	layout := rm.NewLayoutWithSchema(schema)
	tcat, err := rm.NewTableScan(tx, "tblcat", t.tcatLayout) //开辟一张表
	if err != nil {
		return err
	}
	tcat.Insert()                              //往当前区块获得一个可插入的slot
	tcat.SetString("tblname", tblName)         //写入这个的表名
	tcat.SetInt("slotsize", layout.SlotSize()) //写入这个记录的大小
	tcat.Close()                               //操作完就把表给关闭了

	fcat, err := rm.NewTableScan(tx, "fldcat", t.fcatLayout) //创建一张fcat表这个是对这个表的元数据进行管理
	if err != nil {
		return err
	}
	defer fcat.Close() //打开一个scan后要即使关闭这个，否则就阻塞住了
	for _, fieldName := range schema.Fields() {
		fcat.Insert() //在该表中添加一个槽位
		//把表的每个记录都添加到表中
		fcat.SetString("tblname", tblName)
		fcat.SetString("fldname", fieldName)
		fcat.SetInt("type", int(schema.Type(fieldName))) //从schema中获得某个fiedld类型
		fcat.SetInt("length", schema.Length(fieldName))
		fcat.SetInt("offset", layout.Offset(fieldName))
	}
	//以后我们就需要从这两个表中，将特定的记录拿出来
	return nil
}

//GetLayout 获得给定表的Layout结构,从当前的tblcat和fldcat中获得某个具体表的表结构（tblcat表中存储的就是当前表的表名和一个记录的大小，fldcat存储的就是当前的表的各种字段信息）
func (t *TableManager) GetLayout(tblName string, tx *tx.Transaction) (*rm.Layout, error) {
	size := -1
	//从tblcat获得表中一条记录的长度
	tcat, err := rm.NewTableScan(tx, "tblcat", t.tcatLayout) //获得表对应的记录描述
	if err != nil {
		return nil, err
	}
	//遍历这个tblcat表，找到报行这个名称的记录,再通过这个表获得这张表一条记录的大小 TODO 之后可以使用索引加速查询
	for tcat.Next() {
		if tcat.GetString("tblname") == tblName {
			//如果表名就是我们需要的
			size = tcat.GetInt("slotsize") //获得相应表的一条记录的大小
			break
		}
	}
	tcat.Close()
	//从fldcat获得这张表的结构
	sch := rm.NewSchema()                                    //这个管理这个表信息
	offsets := make(map[string]int)                          //记录每个字段在表中的偏移
	fcat, err := rm.NewTableScan(tx, "fldcat", t.fcatLayout) //从这个表中获得这个表的信息
	if err != nil {
		return nil, err
	}
	for fcat.Next() {
		//TODO 这样遍历fldname全局遍历，效率非常低，可以进行优化,B+ index
		if fcat.GetString("tblname") == tblName {
			//这样就得到了这个表的所有信息
			fldName := fcat.GetString("fldname")
			ftype := fcat.GetInt("type")
			length := fcat.GetInt("length")
			offset := fcat.GetInt("offset")
			//写入到map中去
			offsets[fldName] = offset
			sch.AddField(fldName, rm.FIELD_TYPE(ftype), length) //将字段添加到schema中
		}
	}
	fcat.Close()
	//使用tblcat获得的record的size，和fldcat种得到的record的schema构造layout对象
	return rm.NewLayout(sch, offsets, size), nil

}
