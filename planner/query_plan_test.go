package planner

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	bm "miniSQL/buffer_manager"
	fm "miniSQL/file_manager"
	lm "miniSQL/log_manager"
	mm "miniSQL/metadata_manager"
	"miniSQL/parser"
	"miniSQL/query"
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
	"os"
	"testing"
)

func createStudentTable2(tx *tx.Transaction) *mm.MetaDataManager {
	sch := rm.NewSchema() //先创建一个表的结构
	mdm, _ := mm.NewMetaDataManager(false, tx)

	//第一张表学生表
	sch.AddStringField("name", 16) //给当前的表结构添加字段,当前的长度为16
	sch.AddIntField("id")
	layout := rm.NewLayoutWithSchema(sch) //获得一行记录的结构

	ts, _ := rm.NewTableScan(tx, "student", layout) //构造一个表
	ts.BeforeFirst()                                //移动到第一个位置开始进行读取或写入
	for i := 1; i <= 3; i++ {
		ts.Insert() //指向一个可用的插槽

		ts.SetInt("id", i)
		if i == 1 {
			ts.SetString("name", "Tom")
		}
		if i == 2 {
			ts.SetString("name", "Jim")
		}
		if i == 3 {
			ts.SetString("name", "John")
		}
	}
	mdm.CreateTable("student", sch, tx) //创建一张student表，放到meta里面进行管理

	//学生成绩表（学生ID，考试科目，成绩）
	exam_sch := rm.NewSchema()
	exam_sch.AddIntField("stuid") //
	exam_sch.AddStringField("exam", 16)
	exam_sch.AddStringField("grad", 16)
	exam_layout := rm.NewLayoutWithSchema(exam_sch)  //获得考试表一条记录的记录格式
	ts, _ = rm.NewTableScan(tx, "exam", exam_layout) //获得对exam进行操作的Scan
	ts.BeforeFirst()

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 1)
	ts.SetString("exam", "math")
	ts.SetString("grad", "A")

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 1)
	ts.SetString("exam", "algorithm")
	ts.SetString("grad", "B")

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 2)
	ts.SetString("exam", "writing")
	ts.SetString("grad", "C")

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 2)
	ts.SetString("exam", "physics")
	ts.SetString("grad", "C")

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 3)
	ts.SetString("exam", "chemical")
	ts.SetString("grad", "B")

	ts.Insert() //指向一个可用插槽
	ts.SetInt("stuid", 3)
	ts.SetString("exam", "english")
	ts.SetString("grad", "C")
	mdm.CreateTable("exam", exam_sch, tx) //创建一张exam表，放到meta里面进行管理
	return mdm
}

func TestQueryPlan(t *testing.T) {
	fmgr, err := fm.NewFileManager("/home/zevin/query_plan_test", 2048)
	defer func() {
		os.RemoveAll("/home/zevin/query_plan_test")
	}()
	lmgr, err := lm.NewLogManager(fmgr, "logfile")
	assert.Nil(t, err)
	bmgr := bm.NewBufferManager(fmgr, lmgr, 10) //开辟一个缓存管理器，内存池,供使用
	tx := tx.NewTransaction(fmgr, lmgr, bmgr)   //存放入文件管理，日志管理，内存池管理
	//创建一个可以用来学生表和成绩表,通过学生ID进行管理,给表中添加数据
	mdm := createStudentTable2(tx)

	//一个SQL语句
	queryStr := "select name from student,exam where id=stuid and grad=\"A\""
	p := parser.NewSQLParser(queryStr)                //传入当前的SQL语句，并获得对应的SQL解析流
	queryData, _ := p.Query()                         //获得对应的抽象语法树
	testPlanner := NewBasicQueryPlan(mdm)             //构造一个BasicQuery对象
	testPlan := testPlanner.CreatePlan(queryData, tx) //获得执行计划
	testInterface, _ := testPlan.Open()               //启动执行计划
	testScan, ok := testInterface.(query.Scan)        //将他转化成Scan类型的对象
	println(ok)
	for testScan.Next() {
		//调用每个算子的next方法，并输出结果
		fmt.Printf("name: %s\n", testScan.GetString("name"))
	}

}
