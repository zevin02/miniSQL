package metadata_manager

import (
	"fmt"
	"github.com/axiomhq/hyperloglog"
	"github.com/stretchr/testify/assert"
	"math/rand"
	bm "miniSQL/buffer_manager"
	fm "miniSQL/file_manager"
	lm "miniSQL/log_manager"
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
	"os"
	"testing"
)

func TestMetaDataManager(t *testing.T) {
	fmgr, _ := fm.NewFileManager("/home/zevin/meta_test", 400)
	defer os.RemoveAll("/home/zevin/meta_test")
	lmgr, _ := lm.NewLogManager(fmgr, "logfile")
	bmgr := bm.NewBufferManager(fmgr, lmgr, 3)
	tx := tx.NewTransaction(fmgr, lmgr, bmgr)
	sch := rm.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	sch.AddIntField("C")
	mdm, err := NewMetaDataManager(true, tx)
	assert.Nil(t, err)
	mdm.CreateTable("mytable", sch, tx) //创建一张表

	layout := rm.NewLayoutWithSchema(sch)
	size := layout.SlotSize()
	assert.Equal(t, 41, size)
	sch2 := layout.Schema() //从layout中获得对应的表结构
	for _, fldName := range sch2.Fields() {
		fldType := ""
		if sch2.Type(fldName) == rm.INTEGER {
			fldType = "int"
		} else {
			strlen := sch2.Length(fldName)
			fldType = fmt.Sprintf("varchar (%d)", strlen)
		}
		fmt.Printf("%s : %s\n", fldName, fldType)
	}
	ts, err := rm.NewTableScan(tx, "mytable", layout) //获得这张表的结构，并进行CRUD

	assert.Nil(t, err)
	for i := 0; i < 50; i++ {
		ts.Insert()
		n := rand.Intn(50)
		ts.SetInt("A", n)
		ts.SetInt("C", i)

		ts.SetString("B", fmt.Sprintf("rec%d", n))
	}
	for i := 45; i < 55; i++ {
		ts.Insert()
		ts.SetInt("C", i)
	}
	//统计元数据
	si, err := mdm.GetStatInfo("mytable", layout, tx)
	assert.Nil(t, err)
	assert.Equal(t, 7, si.BLockAccessed())
	assert.Equal(t, 60, si.RecordsOutput())
	fmt.Println("distinct for a mytable is ", si.DistinctValue("A"))
	fmt.Println("distinct for b mytable is ", si.DistinctValue("B"))
	assert.Equal(t, 55, si.DistinctValue("C"))
	//统计视图信息
	viewDef := "select B from mytable where A=1"
	mdm.CreateView("viewA", viewDef, tx)
	v, err := mdm.GetViewDef("viewA", tx)
	assert.Nil(t, err)
	assert.Equal(t, viewDef, v)
	tx.Commit()
}
func TestMetaDataManager2(t *testing.T) {
	fmgr, _ := fm.NewFileManager("/home/zevin/meta_test", 400)
	defer os.RemoveAll("/home/zevin/meta_test")

	lmgr, _ := lm.NewLogManager(fmgr, "logfile")
	bmgr := bm.NewBufferManager(fmgr, lmgr, 3)
	tx := tx.NewTransaction(fmgr, lmgr, bmgr)

	mdm, err := NewMetaDataManager(true, tx)
	assert.Nil(t, err)
	viewDef := "select B from mytable where A=1"
	mdm.CreateView("viewA", viewDef, tx)
	v, err := mdm.GetViewDef("viewA", tx)
	assert.Nil(t, err)
	assert.Equal(t, viewDef, v)
	tx.Commit()
}

func TestHyperLogLog(t *testing.T) {
	var fldData map[string]*hyperloglog.Sketch
	fldData = make(map[string]*hyperloglog.Sketch)
	sketch, ok := fldData["12"]
	if !ok {
		sketch = hyperloglog.New16()
		fldData["12"] = sketch
	}
	for i := 0; i < 10; i++ {
		sketch.Insert([]byte(fmt.Sprintf("as%d", i)))
	}
	for i := 0; i < 5; i++ {
		sketch.Insert([]byte(fmt.Sprintf("as%d", i)))
	}
	fmt.Println(sketch.Estimate())
}
