package record_manager

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	bm "miniSQL/buffer_manager"
	fm "miniSQL/file_manager"
	lm "miniSQL/log_manager"
	tx "miniSQL/transaction"
	"testing"
)

//
func TestRecordPage(t *testing.T) {
	fmgr, _ := fm.NewFileManager("/home/zevin/record_test", 400)
	lmgr, _ := lm.NewLogManager(fmgr, "logfile")
	bmgr := bm.NewBufferManager(fmgr, lmgr, 3)
	tx := tx.NewTransaction(fmgr, lmgr, bmgr)
	sch := NewSchema()
	//创建了A和B两个field
	sch.AddIntField("A")
	sch.AddStringField("B", 9)
	layout := NewLayoutWithSchema(sch) //数据都添加进去了，测试他的偏移
	offset := 8
	for _, fieldName := range sch.Fields() {
		assert.Equal(t, offset, layout.Offset(fieldName))
		offset += layout.Offset(fieldName)
		fmt.Printf("%s hash offset %d\n", fieldName, offset)
	}
	blk, err := tx.Append("testfile") //给该文件增加一个区块
	assert.Nil(t, err)
	tx.Pin(blk)                                           //当前事务占用这个文件
	rp := NewRecordPage(tx, blk, LayoutInterface(layout)) //初始化一个记录管理器
	rp.Format()                                           //先将该页面格式化一下
	fmt.Println("Filling the page with random records")
	slot := rp.InsertAfter(-1)     //找到第一条可用的插槽
	valForFieldA := make([]int, 0) //数据将插入到这个里面
	for slot >= 0 {
		n := rand.Intn(50)
		valForFieldA = append(valForFieldA, n) //将当前A的值都添加到这里，供后面使用
		rp.SetInt(slot, "A", n)                //给当前槽位添加上这个记录
		rp.SetString(slot, "B", fmt.Sprintf("rec%d", n))
		slot = rp.InsertAfter(slot) //获得下一个可用的槽位
	}
	//测试插入的字段是否正确
	slot = rp.NextAfter(-1) //找到第一个有效的slot
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		assert.Equal(t, a, valForFieldA[slot])
		assert.Equal(t, b, fmt.Sprintf("rec%d", a))
		slot = rp.NextAfter(slot)
	}
	fmt.Println("Delete these records with A-value <25")
	count := 0
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		if a < 25 {
			//删除
			count++
			fmt.Printf("slot %d :{%d,%s}\n", slot, a, b)
			rp.Delete(slot)
		}
		slot = rp.NextAfter(slot)
	}
	fmt.Printf("%d values under 25 were deleted\n", count)
	fmt.Println("Here are the remain records")
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		assert.True(t, a >= 25)
		fmt.Printf("slot %d :{%d,%s}\n", slot, a, b)
		slot = rp.NextAfter(slot)
	}
	tx.Unpin(blk) //使用完就解释占用
	tx.Commit()
}
