package metadata_manager

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	bm "miniSQL/buffer_manager"
	fm "miniSQL/file_manager"
	lm "miniSQL/log_manager"
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
	"os"
	"testing"
)

func TestNewTableManager(t *testing.T) {
	fmgr, _ := fm.NewFileManager("/home/zevin/table_test", 400)
	defer os.RemoveAll("/home/zevin/table_test")

	lmgr, _ := lm.NewLogManager(fmgr, "logfile")
	bmgr := bm.NewBufferManager(fmgr, lmgr, 3)
	tx := tx.NewTransaction(fmgr, lmgr, bmgr)
	sch := rm.NewSchema()
	//创建了A和B两个field
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	tmgr, err := NewTableManager(true, tx) //创建一个表管理器
	assert.Nil(t, err)
	tmgr.CreateTable("mytable", sch, tx)         //创建一张表，表的每个字段是A.B
	layout, err := tmgr.GetLayout("mytable", tx) //获得当前表的记录元数据
	size := layout.SlotSize()
	sch2 := layout.Schema() //获得他的结构
	fmt.Println("my table slot size:", size)
	fmt.Println("its field are:")
	for _, fieldName := range sch2.Fields() {
		fldType := ""
		if sch2.Type(fieldName) == rm.INTEGER {
			fldType = "int"
		} else {
			strlen := sch2.Length(fieldName)
			fldType = fmt.Sprintf("varchar( %d )", strlen)
		}
		fmt.Printf("%s : %s\n", fieldName, fldType)
	}

}
