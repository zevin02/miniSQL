package planner

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	bm "miniSQL/buffer_manager"
	"miniSQL/comm"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
	mm "miniSQL/metadata_manager"
	"miniSQL/query"
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
	"os"
	"testing"
)

/*
	创建一张student (sname varchar(16),majorId int,gradyear int)
*/
func createStudentTable(tx *tx.Transaction) (*mm.MetaDataManager, error) {
	sch := rm.NewSchema()
	sch.AddStringField("sname", 16)
	sch.AddIntField("majorId")
	sch.AddIntField("gradyear")
	layout := rm.NewLayoutWithSchema(sch)
	ts, _ := rm.NewTableScan(tx, "student", layout) //创建一张表
	ts.BeforeFirst()
	valForFieldSname := make([]int, 0)
	for i := 0; i < 50; i++ {
		ts.Insert() //指向一个可用插槽
		ts.SetInt("majorId", i)
		ts.SetInt("gradyear", 1990+i)
		valForFieldSname = append(valForFieldSname, i)
		s := fmt.Sprintf("sname_%d", i)
		ts.SetString("sname", s)
		fmt.Printf("inserting into slot %s: {%d , %s}\n", ts.GetRid().ToString(), i, s)
	}
	mdm, err := mm.NewMetaDataManager(false, tx)
	mdm.CreateTable("student", sch, tx) //创建一张student表，放到meta里面进行管理
	return mdm, err
}

func printStats(n int, p Plan) {
	fmt.Printf("Here are the stats for plan p %d\n", n)
	fmt.Printf("\tR(p%d):%d\n", n, p.RecordsOutput())
	fmt.Printf("\tB(p%d):%d\n", n, p.BlockAccessed())
}

func TestPlan1(t *testing.T) {
	fmgr, err := fm.NewFileManager("/home/zevin/plan_test", 2048)
	defer func() {
		os.RemoveAll("/home/zevin/plan_test")
	}()
	lmgr, err := lm.NewLogManager(fmgr, "logfile")
	assert.Nil(t, err)
	bmgr := bm.NewBufferManager(fmgr, lmgr, 10) //开辟一个缓存管理器，内存池,供使用
	tx := tx.NewTransaction(fmgr, lmgr, bmgr)   //存放入文件管理，日志管理，内存池管理
	//创建一个表结构
	mdm, err := createStudentTable(tx)
	assert.Nil(t, err)
	//构造一个tableplan
	p1, err := NewTablePlan(tx, "student", mdm)
	assert.Nil(t, err)
	n := 10
	//构造一个majorid=10的selectPlan
	var terms []*query.Term
	terms = append(terms, query.NewTerm(query.NewExpressionWithFieldName("majorId"), query.NewExpressionWithConstant(comm.NewConstantInt(&n))))
	terms = append(terms, query.NewTerm(query.NewExpressionWithFieldName("majorId"), query.NewExpressionWithFieldName("gradyear")))
	pred1 := query.NewPredicateWithMultiTerms(terms)
	p2 := NewSelectPlan(p1, pred1)

	//构造一个gradyear=2000的selectplan
	n1 := 2000
	term2 := query.NewTerm(query.NewExpressionWithFieldName("gradyear"), query.NewExpressionWithConstant(comm.NewConstantInt(&n1)))
	pred2 := query.NewPredicateWithTerm(term2)
	p3 := NewSelectPlan(p1, pred2)
	c := make([]string, 0)
	c = append(c, "sname")
	c = append(c, "majorId")
	//p4的结构table->select->project
	p4 := NewProjectPlan(p3, c)
	printStats(1, p1)
	printStats(2, p2)
	printStats(3, p3)
	printStats(4, p4)
}

func TestPlan2(t *testing.T) {
	fmgr, err := fm.NewFileManager("/home/zevin/plan_test", 2048)
	defer func() {
		os.RemoveAll("/home/zevin/plan_test")
	}()
	lmgr, err := lm.NewLogManager(fmgr, "logfile")
	assert.Nil(t, err)
	bmgr := bm.NewBufferManager(fmgr, lmgr, 10) //开辟一个缓存管理器，内存池,供使用
	tx := tx.NewTransaction(fmgr, lmgr, bmgr)   //存放入文件管理，日志管理，内存池管理
	//创建一个表结构
	mdm, err := createStudentTable(tx)
	assert.Nil(t, err)
	//构造一个tableplan
	p1, err := NewTablePlan(tx, "student", mdm)
	assert.Nil(t, err)

	//构造一个gradyear=2000的selectplan
	n1 := 2000
	term2 := query.NewTerm(query.NewExpressionWithFieldName("gradyear"), query.NewExpressionWithConstant(comm.NewConstantInt(&n1)))
	pred2 := query.NewPredicateWithTerm(term2)
	p3 := NewSelectPlan(p1, pred2)
	c := make([]string, 0)
	c = append(c, "sname")
	c = append(c, "majorId")
	//p4的结构table->select->project
	p4 := NewProjectPlan(p1, c)

	n := 10
	//构造一个majorid=10的selectPlan
	var terms []*query.Term
	terms = append(terms, query.NewTerm(query.NewExpressionWithFieldName("majorId"), query.NewExpressionWithConstant(comm.NewConstantInt(&n))))
	terms = append(terms, query.NewTerm(query.NewExpressionWithFieldName("majorId"), query.NewExpressionWithFieldName("gradyear")))
	pred1 := query.NewPredicateWithMultiTerms(terms)
	//p2的查询树，table->project->select
	p2 := NewSelectPlan(p4, pred1)

	printStats(1, p1)
	printStats(2, p2)
	printStats(3, p3)
	printStats(4, p4)
}
