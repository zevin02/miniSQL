package planner

import (
	"fmt"
	bm "miniSQL/buffer_manager"
	fm "miniSQL/file_manager"
	lm "miniSQL/log_manager"
	mm "miniSQL/metadata_manager"
	"miniSQL/parser"
	"miniSQL/query"
	tx "miniSQL/transaction"
	"os"
	"testing"
)

func PrintStudentTable(tx *tx.Transaction, mdm *mm.MetaDataManager) {
	querySQL := "select name,majorId,gradyear from student "
	p := parser.NewSQLParser(querySQL)
	queryData, _ := p.Query() //获得查询的语法树

	testPlanner := NewBasicQueryPlan(mdm) //创建一个查询计划
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

//name varchar(16),majorId int,gradyear int
func CreateInsertUpdateByUpdatePlanner() {
	fmgr, _ := fm.NewFileManager("/home/zevin/plan_test", 2048)
	defer func() {
		os.RemoveAll("/home/zevin/plan_test")
	}()
	lmgr, _ := lm.NewLogManager(fmgr, "logfile")
	bmgr := bm.NewBufferManager(fmgr, lmgr, 3)
	tx := tx.NewTransaction(fmgr, lmgr, bmgr)
	mdm, _ := mm.NewMetaDataManager(true, tx)

	//使用这个update接口可以进行增加删除修改操作
	updatePlanner := NewBasicUpdatePlanner(mdm) //构造一个updatePlanner对象
	//创建一个STUDENT 表
	createTableSQL := "create table student (name varchar(16),majorId int,gradyear int)"
	//先构造一个SQL解析器
	p := parser.NewSQLParser(createTableSQL)

	upCmd, _ := p.UpdateCmd()                    //根据update操作，生成一个接口类型，并进行返回
	tableData := upCmd.(*parser.CreateTableData) //转化成一个createtabledata对象
	//获得了一个tableData对象，再进行去调用这个对象
	//实现插入的操作
	updatePlanner.ExecuteCreateTable(tableData, tx)
	//向这个地方插入数据

	insertSQL := "insert into student (name,majorId,gradyear) values (\"htylor\",30,2020)"
	p = parser.NewSQLParser(insertSQL)
	upCmd, _ = p.UpdateCmd()
	insertData := upCmd.(*parser.InsertData)

	//插入了一条数据
	updatePlanner.ExecuteInsert(insertData, tx)

	//插入第二条
	insertSQL = "insert into student (name,majorId,gradyear) values (\"jtom\",35,2022)"
	p = parser.NewSQLParser(insertSQL)
	upCmd, _ = p.UpdateCmd()
	insertData = upCmd.(*parser.InsertData)

	//插入了一条数据
	updatePlanner.ExecuteInsert(insertData, tx)

	//打印一下这个表的结构
	PrintStudentTable(tx, mdm)

	//执行一个update操作
	updateSQL := "update student set majorId=20 where majorId=30 and gradyear=2020"
	p = parser.NewSQLParser(updateSQL)
	upCmd, _ = p.UpdateCmd()
	updateData := upCmd.(*parser.UpdateData)
	updatePlanner.ExecuteModify(updateData, tx)
	//打印一下输出的结构
	fmt.Println("table after update:")
	PrintStudentTable(tx, mdm)

	//测试一下delete语句
	deleteSQL := "delete from student where majorId=35"
	p = parser.NewSQLParser(deleteSQL)
	upCmd, _ = p.UpdateCmd()
	deleteData := upCmd.(*parser.DeleteData)
	updatePlanner.ExecuteDelete(deleteData, tx)
	fmt.Println("table after delete:")
	PrintStudentTable(tx, mdm)

}

func TestUpdatePlanner(t *testing.T) {

	CreateInsertUpdateByUpdatePlanner()

}
