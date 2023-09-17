package test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	bm "miniSQL/buffer_manager"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
	"miniSQL/parser"
	"miniSQL/query"
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
	"testing"
)

func TestQuery(t *testing.T) {
	fmgr, err := fm.NewFileManager("/home/zevin/query_test", 400)
	lmgr, err := lm.NewLogManager(fmgr, "logfile")
	assert.Nil(t, err)
	bmgr := bm.NewBufferManager(fmgr, lmgr, 10) //开辟一个缓存管理器，内存池,供使用
	tx1 := tx.NewTransaction(fmgr, lmgr, bmgr)  //存放入文件管理，日志管理，内存池管理
	//创建一个表结构
	sch := rm.NewSchema()
	sch.AddStringField("name", 16)
	sch.AddIntField("age")
	sch.AddIntField("id")
	layout := rm.NewLayoutWithSchema(sch)
	ts, err := rm.NewTableScan(tx1, "student", layout) //创建一张表
	assert.Nil(t, err)
	ts.BeforeFirst()
	//插入3条记录：
	//1:（“jim”,16,233）
	ts.Insert()
	ts.SetString("name", "jim")
	ts.SetInt("age", 16)
	ts.SetInt("id", 233)
	//2:（“tom”,18,567）
	ts.Insert()
	ts.SetString("name", "tom")
	ts.SetInt("age", 18)
	ts.SetInt("id", 567)
	//3:（“hanmeimei”,19,890）
	ts.Insert()
	ts.SetString("name", "hanmeimei")
	ts.SetInt("age", 19)
	ts.SetInt("id", 890)

	ts.Insert()
	ts.SetString("name", "zevin")
	ts.SetInt("age", 1259)
	ts.SetInt("id", 890)
	sql := "SELECT name,age FROM student WHERE id=890"
	sqlParser := parser.NewSQLParser(sql) //构造sql解析器
	queryData := sqlParser.Query()        //实现select解析，得到相应select需要的字段和条件和表名字
	//根据querydata分别构造tablescan，selectscan，projectscan，并执行sql语句
	//创建查询树最底层的数据表节点
	tableScan, err := rm.NewTableScan(tx1, "student", layout)
	//构造一个selectScan节点
	selectScan := query.NewSelectScan(tableScan, queryData.Pred())
	//构造一个projectscan节点
	projectScan := query.NewProjectScan(selectScan, queryData.Fields())
	//为遍历做初始化
	//project调用selectscan，tablescan
	projectScan.BeforeFirst()
	for true {
		if projectScan.Next() == true {
			fmt.Println("found record!")
			for _, field := range queryData.Fields() {
				conVal, err := projectScan.GetVal(field)
				if err != nil {
					fmt.Println("get val err err ", err)
				}
				fmt.Printf("field name %s,its value is %s\n", field, conVal.ToString())
			}
		} else {
			break
		}
	}
	fmt.Println("complete sql execute")
}
