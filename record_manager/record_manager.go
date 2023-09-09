package record_manager

import (
	fm "miniSQL/file_manager"
	tx "miniSQL/transaction"
)

//表中存储一个结构需要考虑
/*
	1.一条记录是否应该完全存储在一个block中
	2.一个block是否存储的数据完全都是同一张表
	3.是否每个field的大小都是固定，数据库应该使用固定长度或者可变长度(字符串)的数据类型来存储字段
	4.
*/

type SLOT_FLAG int

const (
	EMPTY SLOT_FLAG = iota //描述当前slot没有被使用了
	USED                   //描述当前slot已经被使用了
)

//RecordPage 使用recordManager来管理记录在页面中的存储
type RecordPage struct {
	tx     *tx.Transaction //使用一个事务，保证数据的原子性和可恢复性
	blk    *fm.BlockId     //管理的是哪个页面
	layout LayoutInterface //当前管理的某个表，每个字段的管理
}

//NewRecordPage 构造一个RecordPage对象来管理日志块
func NewRecordPage(tx *tx.Transaction, blk *fm.BlockId, layout LayoutInterface) *RecordPage {
	rp := &RecordPage{
		tx:     tx,
		blk:    blk,
		layout: layout,
	}
	//把当前的blk给占用
	tx.Pin(blk)
	return rp
}

//获得当前slot在当前block的偏移位置
func (r *RecordPage) offset(slot int) uint64 {
	return uint64(slot * r.layout.SlotSize()) //slot*一条记录的大小，就是当前记录的偏移
}

//Block 当前记录处在的哪个文件块中
func (r *RecordPage) Block() *fm.BlockId {
	return r.blk
}

//GetInt 返回该字段的值,给定记录所在的编号和记录的field
func (r *RecordPage) GetInt(slot int, fieldName string) int {
	fieldPos := r.offset(slot) + uint64(r.layout.Offset(fieldName)) //获得某个recored中的某个field的实际的存储的偏移位置
	val, err := r.tx.GetInt(r.blk, fieldPos)                        //从事务中得到某个值
	if err != nil {
		return -1 //-1说明没有找到
	} else {
		return int(val) //返回找到的值
	}
}

//SetInt 给某个字段设置数据
func (r *RecordPage) SetInt(slot int, fieldName string, val int) {
	fieldPos := r.offset(slot) + uint64(r.layout.Offset(fieldName)) //获得某个recored中的某个field的实际的存储的偏移位置
	r.tx.SetInt(r.blk, fieldPos, int64(val), true)                  //写入事务中，并且保证生成日志
}

//GetString 返回该字段的值,给定记录所在的编号和记录的field
func (r *RecordPage) GetString(slot int, fieldName string) string {
	fieldPos := r.offset(slot) + uint64(r.layout.Offset(fieldName)) //获得某个recored中的某个field的实际的存储的偏移位置
	val, _ := r.tx.GetString(r.blk, fieldPos)
	return val
}

//SetString 给某个字段设置string类型数据
func (r *RecordPage) SetString(slot int, fieldName string, val string) {
	fieldPos := r.offset(slot) + uint64(r.layout.Offset(fieldName)) //获得某个recored中的某个field的实际的存储的偏移位置
	r.tx.SetString(r.blk, fieldPos, val, true)
}

//Format 将所有页面内的记录设置为默认值,将记录设置成默认的值，int类型就设置成0,string类型就设置成“”
//把所有slot都设置为没有被使用
func (r *RecordPage) Format() {
	slot := 0                 //从第一个slot开始进行处理
	for r.isValidSlot(slot) { //保证当前的blk被pin了
		r.tx.SetInt(r.blk, r.offset(slot), int64(EMPTY), false) //设置成没有被使用,同时也不需要生成日志进行回滚
		sch := r.layout.Schema()                                //获得当前schema，并从中获得他的每个fieldname
		for _, fieldName := range sch.Fields() {
			//遍历每个字段
			fieldPos := r.offset(slot) + uint64(r.layout.Offset(fieldName))
			if sch.Type(fieldName) == INTEGER {
				r.tx.SetInt(r.blk, fieldPos, 0, false)
			} else {
				r.tx.SetString(r.blk, fieldPos, "", false)
			}
		}
		slot += 1 //处理完一个record就移动到下一个slot
	}

}

//Delete 删除给定编号的记录,只需要把这个占位符设置为无效即可,设置成0
func (r *RecordPage) Delete(slot int) {
	r.setFlag(slot, EMPTY) //将该槽位设置成无效的
}

//NextAfter 某一条记录都有一个占位符来表示这个记录是否有效
//给出从给定编号之后，flag标志位被设置成1(有效的)的记录的编号,找到下一个有效的slot
func (r *RecordPage) NextAfter(slot int) int {
	//这个是找到就找到，每找到就没找到
	return r.searchAfter(slot, USED)
}

//InsertAfter 查找给定编号在之后，flag标志设置成0（无效）记录的编号,可以使用该位置进行设置记录,查找下一个没有使用的slot
func (r *RecordPage) InsertAfter(slot int) int {
	newSlot := r.searchAfter(slot, EMPTY)
	if newSlot >= 0 {
		//如果找到，就设置，没找到就不设置
		r.setFlag(newSlot, USED)
	}
	return newSlot
}

//setFlag 给某个slot设置标记位
func (r *RecordPage) setFlag(slot int, flag SLOT_FLAG) {
	r.tx.SetInt(r.blk, r.offset(slot), int64(flag), true) //获得他的偏移即可，因为头8字节就是这个record占位符
}

//isValidSlot 判断当前的slot是否是有效的slot
func (r *RecordPage) isValidSlot(slot int) bool {
	//因为我们是保证一个block满了，剩余的数据就不用了
	return r.offset(slot+1) <= r.tx.BlockSize() //只要下一条记录的开始不超过一个block块大小即可
}

//searchAfter 找到当前slot后面符合flag标志的slot
func (r *RecordPage) searchAfter(slot int, flag SLOT_FLAG) int {
	slot += 1
	for r.isValidSlot(slot) {
		//一个一个slot往后面遍历
		val, _ := r.tx.GetInt(r.blk, r.offset(slot)) //得到某个slot的占位符，判断有效还是无效
		if SLOT_FLAG(val) == flag {
			return slot
		}
		slot += 1
	}
	//-1说明没有找到
	return -1
}
