package record_manager

import (
	fm "miniSQL/file_manager"
	tx "miniSQL/transaction"
)

//TableScan 对一张表进行读写操作
type TableScan struct {
	tx          *tx.Transaction        //使用一个事务来进行出来操作
	layout      LayoutInterface        //管理当条记录
	rp          RecordManagerInterface //记录管理器
	fileName    string                 //管理当前记录的文件名，一个表，全部存储在一个文件中
	currentSlot int                    //当前表处在的槽位
}

//NewTableScan 构造该表的记录扫描器
func NewTableScan(tx *tx.Transaction, tableName string, layout LayoutInterface) (*TableScan, error) {
	tableScan := &TableScan{
		tx:       tx,
		layout:   layout,
		fileName: tableName + ".tbl", //一个表都存储在".tbl"文件中
	}
	size, err := tx.Size(tableScan.fileName) //获得当前文件占用了多少个区块
	if err != nil {
		panic(err)
	}
	if size == 0 {
		//当前文件为空,增加一个区块
		err := tableScan.Move2NewBlock()
		if err != nil {
			return nil, err
		}
	} else {
		//读取第一个区块
		tableScan.Move2Block(0)
	}
	return tableScan, nil
}

//Move2NewBlock 给当前文件增加一个区块上来
func (t *TableScan) Move2NewBlock() error {
	t.Close()                           //
	blk, err := t.tx.Append(t.fileName) //当前事务给这个文件增加一个区块
	if err != nil {
		return err
	}
	//给日志管理器放入一个新的区块
	t.rp = NewRecordPage(t.tx, blk, t.layout) //在当前事务中，对blk的record进行管理
	t.currentSlot = -1                        //当前还没有进行任何查找有效的数据
	return nil
}

//BeforeFisrt 在第一次读取之前进行调用
func (t *TableScan) BeforeFisrt() {
	t.Move2Block(0) //
}

//Close 释放掉当前日志管理器中的区块
func (t *TableScan) Close() {
	if t.rp != nil {
		t.tx.Unpin(t.rp.Block()) //将当前的日志管理器管理的blk给释放掉
	}
}

//Move2Block 读取某个区块的数据(已经开辟出来了),让日志管理器进行管理
func (t *TableScan) Move2Block(blkNum int) {
	t.Close()
	blk := fm.NewBlockId(t.fileName, uint64(blkNum)) //构造一个新的区块
	t.rp = NewRecordPage(t.tx, blk, t.layout)        //更新日志管理器,之前日志管理器的
	t.rp.Format()                                    //将当前块进行初始化
	t.currentSlot = -1
}

//Insert 将当前slot指向下一个可用的槽位EMPTY,如果当前区块slot已经用完了，就会开辟一个新的区块继续读取数据
func (t *TableScan) Insert() {
	t.currentSlot = t.rp.InsertAfter(t.currentSlot) //在当前slot查找下一个可用的slot
	//该函数成功就返回指定的slot位，-1表示没有找到
	for t.currentSlot < 0 {
		//当前区块找不到下一个可用的槽位
		if t.AtLastBlock() {
			//当前处在最后一个block
			t.Move2NewBlock()
		} else {
			//当前并不是处在最后一个区块，前面已经开辟好区块了，可以直接读取
			t.Move2Block(int(t.rp.Block().Number() + 1)) //读取当前日志管理器管理的下一个区块
		}
		t.currentSlot = t.rp.InsertAfter(t.currentSlot) //获得当前区块的最新可插入的slot
	}
}

func (t *TableScan) AtLastBlock() bool {
	size, err := t.tx.Size(t.fileName)
	if err != nil {
		panic(err)
	}
	//如果当前文件=400,则size=1,当前的blocknum应该是0，如果相等的话，就应该处在最后一个区块了
	return t.rp.Block().Number() == size-1
}

//增删改查一个表的操作都是由记录管理器来操作处理

//SetInt 插入一个Int 数据
func (t *TableScan) SetInt(fieldName string, val int) {
	t.rp.SetInt(t.currentSlot, fieldName, val)
}

//SetString 插入一个string数据
func (t *TableScan) SetString(fieldName string, val string) {
	t.rp.SetString(t.currentSlot, fieldName, val)
}

//GetInt 读取当前slot的int数据
func (t *TableScan) GetInt(fieldName string) int {
	return t.rp.GetInt(t.currentSlot, fieldName)
}
func (t *TableScan) GetString(fieldName string) string {
	return t.rp.GetString(t.currentSlot, fieldName)
}

//Delete 删除当前slot的数据
func (t *TableScan) Delete() {
	t.rp.Delete(t.currentSlot)
}
