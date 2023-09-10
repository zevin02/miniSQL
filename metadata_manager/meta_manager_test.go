package metadata_manager

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	bm "miniSQL/bufferManager"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
	"testing"
)

func TestMetaDataManager(t *testing.T) {
	fmgr, _ := fm.NewFileManager("/home/zevin/meta_test", 400)
	lmgr, _ := lm.NewLogManager(fmgr, "logfile")
	bmgr := bm.NewBufferManager(fmgr, lmgr, 3)
	tx := tx.NewTransaction(fmgr, lmgr, bmgr)
	sch := rm.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)

	mdm, err := NewMetaDataManager(true, tx)
	assert.Nil(t, err)
	mdm.CreateTable("mytable", sch, tx) //创建一张表

	layout := rm.NewLayoutWithSchema(sch)
	size := layout.SlotSize()
	assert.Equal(t, 33, size)
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
	ts, err := rm.NewTableScan(tx, "mytable", layout) //获得这张表的结构，并进行CRUD操作
	assert.Nil(t, err)
	for i := 0; i < 50; i++ {
		ts.Insert()
		n := rand.Intn(50)
		ts.SetInt("A", n)
		ts.SetString("B", fmt.Sprintf("rec%d", n))
	}
	//统计元数据
	si, err := mdm.GetStatInfo("mytable", layout, tx)
	assert.Nil(t, err)
	assert.Equal(t, 5, si.BLockAccessed())
	assert.Equal(t, 50, si.RecordsOutput())
	fmt.Println("distinct for a mytable is ", si.DistinctValue("A"))
	fmt.Println("distinct for b mytable is ", si.DistinctValue("B"))
	//统计视图信息
	viewDef := "select B from mytable where A =1"
	mdm.CreateView("viewA", viewDef, tx)
	v, err := mdm.GetViewDef("viewA", tx)
	assert.Nil(t, err)
	assert.Equal(t, viewDef, v)
	tx.Commit()
}
