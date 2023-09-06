package transaction

import (
	bm "miniSQL/bufferManager"
	fm "miniSQL/file_manager"
)

//对pin过的buffer进行简单的管理

type BufferList struct {
	buffers  map[*fm.BlockId]*bm.Buffer //当前已经pin的Buffer
	buffeMgr *bm.BufferManager          //缓存管理器
	pins     []*fm.BlockId              //所以已经被pin的blockId都存储起来
}

//NewBufferList 构造一个BufferList
func NewBufferList(bufferMgr *bm.BufferManager) *BufferList {
	return &BufferList{
		buffers:  make(map[*fm.BlockId]*bm.Buffer),
		buffeMgr: bufferMgr,
		pins:     make([]*fm.BlockId, 0),
	}
}

//getBuf 从bufferList中得到指定的blk区块的数据
func (b *BufferList) getBuf(blk *fm.BlockId) *bm.Buffer {
	buff, _ := b.buffers[blk]
	return buff
}

//Pin 将当前的blockid进行开辟获得
func (b *BufferList) Pin(blk *fm.BlockId) error {
	buff, err := b.buffeMgr.Pin(blk) //调用缓存管理器对buffer进行获得
	if err != nil {
		return err
	}
	b.buffers[blk] = buff        //将当前得到的已经pin过的buffer添加到bufferlist中进行管理
	b.pins = append(b.pins, blk) //添加当前区块进行管理,每次尽管当前的blk已经存在了，同样还是会增加该blk进去
	return nil
}

//将指定buffer中的数据进行UNPIN掉
func (b *BufferList) Unpin(blk *fm.BlockId) {
	//先从已经pin的缓存中检查是否存在，如果不存在，就说明该区块都没有pin过，就直接返回
	buff, ok := b.buffers[blk]
	if !ok {
		//当前blk没有被pin过
		return
	}
	//当前的blk被pin过了，就需要使用缓存管理器将他取消pin
	b.buffeMgr.Unpin(buff) //将当前buff进行unpin掉
	for idx, pinnedBlk := range b.pins {
		if pinnedBlk == blk {
			//如果找到了当前的pin的id
			b.pins = append(b.pins[:idx], b.pins[idx+1:]...) //将idx位置的数据进行删除
		}
	}
	delete(b.buffers, blk) //将该节点进行删除
}

//UnpinAll unpin掉当前事务使用的所有的缓存页面
func (b BufferList) UnpinAll() {
	for _, blk := range b.pins { //获得所有pin的对象
		buffer := b.buffers[blk]
		b.buffeMgr.Unpin(buffer)
	}
	//垃圾回收器会将内存进行一个回收
	b.buffers = make(map[*fm.BlockId]*bm.Buffer) //设置一个新的对象
	b.pins = make([]*fm.BlockId, 0)              //当前的pin也重新设置

}
