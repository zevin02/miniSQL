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
	fmgr, err := fm.NewFileManager("/home/zevin/tablescan_test", 400)
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
	ts.BeforeFirst() //把第一块数据读取上来
	valForFieldA := make([]int, 0)
	//往表中插入数据，当前的缓存页表被占用的时候，该页表中的数据就会被刷新到磁盘中
	for i := 0; i < 100000; i++ {
		ts.Insert() //指向当前blk的可用的插槽,i=12
		n := rand.Intn(50)
		ts.SetInt("A", n) //往表中插入数据
		valForFieldA = append(valForFieldA, n)
		s := fmt.Sprintf("rec%d", n)
		ts.SetString("B", s)
		fmt.Printf("insert into slot %s:{%d ,%s}\n", ts.GetRid().ToString(), n, s)
	}
	ts.BeforeFirst() //回到表的第一个区块来处理数据
	//测试插入的是否正确
	slot := 0
	for ts.Next() {
		//遍历当前表的数据
		//获得当前记录的各个字段
		a := ts.GetInt("A")
		b := ts.GetString("B")
		assert.Equal(t, a, valForFieldA[slot])
		assert.Equal(t, b, fmt.Sprintf("rec%d", a))
		slot += 1
	}
	fmt.Println("Deleting records with A-values < 25")
	count := 0
	ts.BeforeFirst() //再重新回到起点,对数据进行删除,把小于25的删除
	for ts.Next() {
		//遍历当前表的数据
		//获得当前记录的各个字段
		a := ts.GetInt("A")
		b := ts.GetString("B")
		if a < 25 {
			count += 1
			fmt.Printf("slot: %s:{%d ,%s}\n", ts.GetRid().ToString(), a, b)
			ts.Delete() //删除当前的记录
		}
	}
	fmt.Println("Here are the remaining records:")
	ts.BeforeFirst()
	//测试当前剩余的值都是大于25的
	for ts.Next() {
		//可能当前块也都已经被删除数据了
		a := ts.GetInt("A")
		b := ts.GetString("B")
		assert.True(t, a >= 25)
		fmt.Printf("slot %s :{%d ,%s}\n", ts.GetRid(), a, b)
	}
	ts.Close()
	tx.Commit()
}
