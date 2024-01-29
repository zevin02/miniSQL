package planner

import (
	mm "miniSQL/metadata_manager"
	"miniSQL/parser"
	"miniSQL/query"
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
)

//因为无论是修改还是删除，创建还是插入，都是进行修改，所以在这个文件进行处理即可
//update student set majorId=20 where majorId=30 and gradyear=2020
//要完成上面操作，需要scan底层的文件快，找到所有满足where条件的记录，如果语句是update,就把找到的记录修改掉，如果是delete，就把找到的记录删除掉

type BasicUpdatePlanner struct {
	mdm *mm.MetaDataManager
}

//NewBasicUpdatePlanner 构造一个当前的这个类对象
func NewBasicUpdatePlanner(mdm *mm.MetaDataManager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{
		mdm: mdm,
	}
}

//ExecuteDelete 执行删除操作,返回删除的记录的数量
func (b *BasicUpdatePlanner) ExecuteDelete(data *parser.DeleteData, tx *tx.Transaction) int {
	//首先要先把要删除的记录给扫描出来
	//构造一个表查询计划
	tablePlan, err := NewTablePlan(tx, data.TableName(), b.mdm) //这个tableplan主要是用来打开底层的数据库的
	if err != nil {
		return 0
	}
	selectPlan := NewSelectPlan(tablePlan, data.Pred()) //这个selectplan主要是用来根据查询条件进行筛选数据的
	//使用一个scan对象把记录拿出来
	scan, err := selectPlan.Open() //把记录拿出来
	if err != nil {
		return 0
	}
	updateScan := scan.(*query.SelectScan) //进行强制类型转化成selectScan对象
	count := 0                             //这个就是记录当前有多少条记录的
	//根据当前的这个updateScan对象，进行向后查找
	for updateScan.Next() {
		//进入到这个地方说明，他当前就是有一条符号条件的记录了
		updateScan.Delete() //删除底层的记录，就是把当前的某个slot位置设置为没有被使用就说明当前已经被删除了
		count++
	}
	return count

}

func (b *BasicUpdatePlanner) ExecuteModify(data *parser.UpdateData, tx *tx.Transaction) int {
	//把记录一条一条的取出来，
	tablePlan, err := NewTablePlan(tx, data.TableName(), b.mdm) //这个tableplan主要是用来打开底层的数据库的
	if err != nil {
		return 0
	}

	selectPlan := NewSelectPlan(tablePlan, data.Pred()) //这个selectplan主要是用来根据查询条件进行筛选数据的
	//使用一个scan对象把记录拿出来
	scan, err := selectPlan.Open() //把记录拿出来
	if err != nil {
		return 0
	}
	updateScan := scan.(*query.SelectScan) //进行强制类型转化成selectScan对象
	count := 0
	//update Student set gradyear=2020 where gradyear=2019
	//下面的evaluate就是把这个要修改的
	//这样的实现就是按照火山模型，把符合条件的记录一条一条的取出来
	for updateScan.Next() {
		val := data.NewValue().Evaluate(scan.(query.Scan)) //获得需要被修改成的值,看看要修改成哪些值，这个操作就是把20给拿出来
		updateScan.SetVal(data.TargetField(), val)         //把特定的字段设置成特定的值
		count++
	}
	return count
}

//ExecuteInsert 执行当前的insert语句，最后返回是否执行成功
func (b *BasicUpdatePlanner) ExecuteInsert(data *parser.InsertData, tx *tx.Transaction) bool {
	tablePlan, err := NewTablePlan(tx, data.TableName(), b.mdm) //这个tableplan主要是用来打开底层的数据库的
	if err != nil {
		return false
	}
	//因为是进行插入，所以就没有select这个操作了
	uScan, err := tablePlan.Open()      //打开这个底层的表
	updateScan := uScan.(*rm.TableScan) //获得这个tableScan对象
	updateScan.Insert()                 //向后增加一个可用的空间
	insertFields := data.Fields()       //获得需要写入的字段
	insertVal := data.Vals()            //获得需要写入的值
	for i := 0; i < len(insertFields); i++ {
		//遍历这个字段名，并把记录进行写入
		updateScan.SetVal(insertFields[i], insertVal[i]) //相应的字段插入进相应的值
	}
	updateScan.Close() //执行完进行一个关闭
	return true        //
}

//ExecuteCreateTable 创建一个表结构，create table
func (b *BasicUpdatePlanner) ExecuteCreateTable(data *parser.CreateTableData, tx *tx.Transaction) error {
	return b.mdm.CreateTable(data.TableName(), data.Schema(), tx)
}

//ExecuteCreateView 创建一个视图
func (b *BasicUpdatePlanner) ExecuteCreateView(data *parser.CreateViewData, tx *tx.Transaction) error {
	return b.mdm.CreateView(data.ViewName(), data.ViewDef(), tx) //创建一个视图
}

//ExecuteCreateIndex 创建一个索引
func (b *BasicUpdatePlanner) ExecuteCreateIndex(data *parser.CreateIndexData, tx *tx.Transaction) error {
	//TODO
	return nil
}
