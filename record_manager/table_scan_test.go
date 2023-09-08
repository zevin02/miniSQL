package record_manager

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	bm "miniSQL/bufferManager"
	fm "miniSQL/file_manager"
	lm "miniSQL/logManager"
	tx "miniSQL/transaction"
	"testing"
)

func TestTableScanInsertAndDelete(t *testing.T) {
	fmgr, err := fm.NewFileManager("/home/zevin/transaction_test", 400)
	lmgr, _ := lm.NewLogManager(fmgr, "logfile")
	bmgr := bm.NewBufferManager(fmgr, lmgr, 3) //开辟一个缓存管理器，内存池,供使用
	assert.Nil(t, err)
	tx := tx.NewTransaction(fmgr, lmgr, bmgr) //打开一个事务
	sch := NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := NewLayoutWithSchema(sch)
	for _, fieldName := range sch.Fields() {
		offset := layout.Offset(fieldName)
		fmt.Printf("%s has offset %d\n", fieldName, offset)
	}
	//创建一张表,一张表里面可以是有多个block块,之前的日志管理器是管理特定的某一个block，tablesacan中的特定block块就是通过recordmgr来进行管理的
	ts, err := NewTableScan(tx, "T", layout)
	assert.Nil(t, err)
	fmt.Println("Filling the table with 50 random records")
	ts.BeforeFisrt() //把第一块数据读取上来
	valForFieldA := make([]int, 0)
	//往表中插入数据
	for i := 0; i < 50; i++ {
		ts.Insert() //指向当前blk的可用的插槽
		n := rand.Intn(50)
		ts.SetInt("A", n) //往表中插入数据
		valForFieldA = append(valForFieldA, n)
		s := fmt.Sprintf("rec%d", n)
		ts.SetString("B", s)
		fmt.Printf("insert into slot %s:{%d ,%s}\n", ts.GetRid().ToString, n, s)
	}

}
