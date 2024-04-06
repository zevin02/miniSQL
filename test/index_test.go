package test

import (
	"fmt"
	bm "miniSQL/buffer_manager"
	"miniSQL/comm"
	fm "miniSQL/file_manager"
	mdm "miniSQL/metadata_manager"

	lm "miniSQL/log_manager"
	"miniSQL/parser"
	"miniSQL/planner"
	"miniSQL/query"
	"miniSQL/record_manager"
	tx "miniSQL/transaction"
	"os"
	"testing"
)

//输出学生表中的数据
func PrintStudentTable(tx *tx.Transaction, mdm *mdm.MetaDataManager) {
	querySQL := "select name,majorId,gradyear from student "
	p := parser.NewSQLParser(querySQL)
	queryData, _ := p.Query() //获得查询的语法树

	testPlanner := planner.NewBasicQueryPlan(mdm) //创建一个查询计划
	//创建一个查询计划
	testPlan := testPlanner.CreatePlan(queryData, tx)
	testInterface, _ := testPlan.Open()
	testScan := testInterface.(query.Scan)
	for testScan.Next() {
		fmt.Printf("name:%s,majorId:%d,gradyear:%d\n",
			testScan.GetString("name"),
			testScan.GetInt("majorId"),
			testScan.GetInt("gradyear"))
	}

}

func TestIndexManager(t *testing.T) {
	fmgr, _ := fm.NewFileManager("/home/zevin/index_test", 4096)
	defer func() {
		os.RemoveAll("/home/zevin/index_test")
	}()
	lmgr, _ := lm.NewLogManager(fmgr, "logfile")

	//这个地方缓冲区如果设置的太小的话，就会出现问题，todo 检查是什么地方出问的问题
	bmgr := bm.NewBufferManager(fmgr, lmgr, 15)
	tx := tx.NewTransaction(fmgr, lmgr, bmgr)
	mdm, _ := mdm.NewMetaDataManager(fmgr.IsNew(), tx)
	//创建student表，并插入一些记录
	updatePlanner := planner.NewBasicUpdatePlanner(mdm) //构造一个updatePlanner对象
	//创建一个STUDENT 表
	createTableSQL := "create table student (name varchar(16),majorId int,gradyear int)"
	//先构造一个SQL解析器
	p := parser.NewSQLParser(createTableSQL)
	upCmd, _ := p.UpdateCmd()                    //根据update操作，生成一个接口类型，并进行返回
	tableData := upCmd.(*parser.CreateTableData) //转化成一个createtabledata对象
	updatePlanner.ExecuteCreateTable(tableData, tx)

	insertSQL := "insert into student (name,majorId,gradyear) values (\"htylor\",30,2020)"
	p = parser.NewSQLParser(insertSQL)
	upCmd, _ = p.UpdateCmd()
	insertData := upCmd.(*parser.InsertData)

	//插入了一条数据
	updatePlanner.ExecuteInsert(insertData, tx)

	insertSQL = "insert into student (name,majorId,gradyear) values (\"jtom\",35,2023)"
	p = parser.NewSQLParser(insertSQL)
	upCmd, _ = p.UpdateCmd()
	insertData = upCmd.(*parser.InsertData)

	//插入了一条数据
	updatePlanner.ExecuteInsert(insertData, tx)
	fmt.Println("table after insert:")
	PrintStudentTable(tx, mdm)
	//在student表的majorid字段上建立索引
	mdm.CreateIndex("majoridIdx", "student", "majorId", tx)
	//查询建立在student表上的索引，并根据索引输出对应的记录信息
	//得到在某个表上的信息
	studentPlan, _ := planner.NewTablePlan(tx, "student", mdm)
	updateScanI, _ := studentPlan.Open() //打开当前的表，进行一个读取数据
	//可以对student这个表进行一个读取了
	updateScan := updateScanI.(*record_manager.TableScan)
	//先获取每个字段对应的索引对象，这里我们是只有majorId上建立了索引对象
	indexes := mdm.GetIndexInfo("student", tx) //得到了当前表的所有的索引相关的信息
	//获取majorId对应的索引对象
	majorIdxInfo := indexes["majorId"] //获得这个对象的索引信息
	//将rid加入到索引表中
	majorIdx := majorIdxInfo.Open()

	updateScan.BeforeFirst()
	//遍历数据表，根据数据表来填充索引表中的记录
	for updateScan.Next() {
		//返回当前记录的rid
		dataRidI := updateScan.GetRid()
		dataRid := dataRidI.(*record_manager.RID)
		dataval := updateScan.GetVal("majorId")
		//向索引数据表中插入一条记录
		majorIdx.Insert(dataval, dataRid)
	}
	//通过索引表获得给定字段内容的记录
	majorId := 35
	//我们要查询的索引数据就是majorId
	majorIdx.BeforeFirst(comm.NewConstantInt(&majorId))
	for majorIdx.Next() {
		//对当前的这个索引表进行遍历，读取真正的有效的索引记录
		datarid := majorIdx.GetDataRID() //查询得到了对应的这条记录所在的位置
		updateScan.Move2Rid(datarid)     //将表扫描器移动到对应的区块上
		fmt.Printf("student name :%s, id: %d\n", updateScan.GetString("name"),
			updateScan.GetInt("majorId"))

	}

}
