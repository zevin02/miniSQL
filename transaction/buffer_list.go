package transaction

import (
	bm "miniSQL/buffer_manager"
	fm "miniSQL/file_manager"
)

//BufferList 对pin过的buffer进行简单的管理
//一个bufferList对应了一个事务
type BufferList struct {
	buffers  map[fm.BlockId]*bm.Buffer //当前已经pin的Buffer
	buffeMgr *bm.BufferManager         //缓存管理器
	pins     map[fm.BlockId]bool       //key对应当前的事务管理的某个区块号，value标识当前是否被使用
}

//NewBufferList 构造一个BufferList
func NewBufferList(bufferMgr *bm.BufferManager) *BufferList {
	return &BufferList{
		buffers:  make(map[fm.BlockId]*bm.Buffer),
		buffeMgr: bufferMgr,
		pins:     make(map[fm.BlockId]bool),
	}
}

//getBuf 从bufferList中得到指定的blk区块的数据
func (b *BufferList) getBuf(blk *fm.BlockId) *bm.Buffer {
	buff, _ := b.buffers[*blk]
	return buff
}

//Pin 将当前的blockid进行开辟获得
func (b *BufferList) Pin(blk *fm.BlockId) error {
	buff, err := b.buffeMgr.Pin(blk) //调用缓存管理器对buffer进行获得
	if err != nil {
		return err
	}
	b.buffers[*blk] = buff //将当前得到的已经pin过的buffer添加到bufferlist中进行管理

	exists, ok := b.pins[*blk]
	if ok && exists {
		//这个就说明这个区块已经被pin过了，就不用再去pin一次
		return nil
	} else {
		//到这里说明这个区块不存在
		b.pins[*blk] = true
		return nil
	}

	//b.pins = append(b.pins, *blk) //添加当前区块进行管理,每次尽管当前的blk已经存在了，同样还是会增加该blk进去
}

//Unpin 将指定buffer中的数据进行UNPIN掉,一次只会去掉一个该对象
func (b *BufferList) Unpin(blk fm.BlockId) {
	//先从已经pin的缓存中检查是否存在，如果不存在，就说明该区块都没有pin过，就直接返回
	buff, ok := b.buffers[blk]
	if !ok {
		//当前blk没有被pin过
		return
	}
	//当前的blk被pin过了，就需要使用缓存管理器将他取消pin
	b.buffeMgr.Unpin(buff) //将当前buff进行unpin掉
	//在map中获得
	_, ok = b.pins[blk]
	if ok {
		//说明当前的区块存在，直接删除即可
		delete(b.pins, blk)
	}

	//for idx, pinnedBlk := range b.pins {
	//	if pinnedBlk == *blk {
	//		//如果找到了当前的pin的id
	//		b.pins = append(b.pins[:idx], b.pins[idx+1:]...) //将idx位置的数据进行删除
	//		break
	//	}
	//}

	delete(b.buffers, blk) //将该节点进行删除
}

//UnpinAll unpin掉当前事务使用的所有的缓存页面
func (b *BufferList) UnpinAll() {
	//遍历当前的map，将当前对应的buffer全部给释放掉
	for blk, _ := range b.pins { //将当前所以出处在pin的对象全部解除pin
		buffer := b.buffers[blk]
		b.buffeMgr.Unpin(buffer)
	}
	//垃圾回收器会将内存进行一个回收
	b.buffers = make(map[fm.BlockId]*bm.Buffer) //设置一个新的对象
	b.pins = make(map[fm.BlockId]bool)          //当前的pin也重新设置

}
