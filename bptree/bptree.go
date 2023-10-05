package bptree

import (
	"sync"
)

//Latch 用来保护某个数据结构进行保护，对节点进行保护,被操作过程中持有的

//BPItem 用来记录数据
type BPItem struct {
	key int64
	val interface{}
}
type Header struct {
	isIndex bool  //是否是索引节点
	isRoot  bool  //是否是根节点
	maxKey  int64 //当前节点第最大值
}

type BPInternalNode struct {
	Nodes []*BPNode //当前非叶子节点存储的多个子节点,叶子节点的Nodes=nil
}

type BPLeafNode struct {
	Items []BPItem //当前存储的
	Next  *BPNode  //叶子节点中指向下一个叶子节点
}

//BPNode 叶子节点和内部节点使用相同的结构
type BPNode struct {
	CommonHeader Header //元数据
	leafNode     *BPLeafNode
	internalNode *BPInternalNode
	lock         sync.RWMutex //TODO 后期改成并发管理器来替代
}

//setValue 向当前的叶子节点种插入元素
func (node *BPNode) setValue(key int64, value interface{}) (isMax bool) {
	item := BPItem{key: key, val: value} //构造一个叶子节点插入的对象
	num := len(node.leafNode.Items)      //获得当前叶子叶子节点原本有多少个值
	if num < 1 {
		//当前叶子节点还没有元素,直接就可以进行插入了
		node.leafNode.Items = append(node.leafNode.Items, item)
		node.CommonHeader.maxKey = item.key //更新当前节点的最大key值
		return false
	} else if key < node.leafNode.Items[0].key {
		//当前的key，比第一个key还要小
		node.leafNode.Items = append([]BPItem{item}, node.leafNode.Items...) //把之前的元素移动到这个元素之后
		return false
	} else if key > node.leafNode.Items[num-1].key {
		//当前的元素比最大的key还要大
		node.leafNode.Items = append(node.leafNode.Items, item) //在当前的最后插入
		node.CommonHeader.maxKey = item.key
		//他的父亲节点的最大值也需要更新

		return true
	}
	//不是上面的这些情况
	//就说明当前的num不是1,同时是在中间进行插入的
	//遍历到比这个小，比这个大的位置
	index := binarySearchItems(node.leafNode.Items, key)
	if node.leafNode.Items[index].key > key {
		//如果当前的比key大，说明找到了位置
		node.leafNode.Items = append(node.leafNode.Items, BPItem{})      //插入一个位置
		copy(node.leafNode.Items[index+1:], node.leafNode.Items[index:]) //往后移动一位
		node.leafNode.Items[index] = item
		return false
	} else if node.leafNode.Items[index].key == key {
		//如果当前的key就等于那个key，直接更新
		node.leafNode.Items[index] = item
		return false
	}
	return false
}

//给当前的节点添加孩子节点
func (node *BPNode) addChild(child *BPNode) {
	num := len(node.internalNode.Nodes)
	if child.leafNode != nil {
		//当前是叶子节
		if num < 1 {
			//当前的节点数量=0,直接添加
			node.internalNode.Nodes = append(node.internalNode.Nodes, child)
			node.CommonHeader.maxKey = child.CommonHeader.maxKey
			return
		} else if child.CommonHeader.maxKey < node.internalNode.Nodes[0].CommonHeader.maxKey {
			//当前比第一个值还小,插入到第一个位置
			node.internalNode.Nodes = append([]*BPNode{child}, node.internalNode.Nodes...)
			return
		} else if child.CommonHeader.maxKey > node.internalNode.Nodes[num-1].CommonHeader.maxKey {
			//比最后一个大，插入到结尾
			node.internalNode.Nodes = append(node.internalNode.Nodes, child)
			node.CommonHeader.maxKey = child.CommonHeader.maxKey
			return
		}
		//在中间,中间插入
		index := binarySearchNodes(node.internalNode.Nodes, child.CommonHeader.maxKey)
		if node.internalNode.Nodes[index].CommonHeader.maxKey > child.CommonHeader.maxKey {
			node.internalNode.Nodes = append(node.internalNode.Nodes, nil)           //添加一个元素
			copy(node.internalNode.Nodes[index+1:], node.internalNode.Nodes[index:]) //向后移动一个位置
			node.internalNode.Nodes[index] = child
			return
		}
	} else {
		if num < 1 {
			//当前的节点数量=0,直接添加
			node.internalNode.Nodes = append(node.internalNode.Nodes, child)
			node.CommonHeader.maxKey = child.CommonHeader.maxKey
			return
		} else if child.CommonHeader.maxKey < node.internalNode.Nodes[0].CommonHeader.maxKey {
			//当前比第一个值还小,插入到第一个位置
			node.internalNode.Nodes = append([]*BPNode{child}, node.internalNode.Nodes...)
			return
		} else if child.CommonHeader.maxKey > node.internalNode.Nodes[num-1].CommonHeader.maxKey {
			//比最后一个大，插入到结尾
			node.internalNode.Nodes = append(node.internalNode.Nodes, child)
			node.CommonHeader.maxKey = child.CommonHeader.maxKey
			return
		}
		//在中间,中间插入
		index := binarySearchNodes(node.internalNode.Nodes, child.CommonHeader.maxKey)
		if node.internalNode.Nodes[index].CommonHeader.maxKey > child.CommonHeader.maxKey {
			node.internalNode.Nodes = append(node.internalNode.Nodes, nil)           //添加一个元素
			copy(node.internalNode.Nodes[index+1:], node.internalNode.Nodes[index:]) //向后移动一个位置
			node.internalNode.Nodes[index] = child
			return
		}
	}

}

//deleteItem 在当前叶子节点中删除一个元素
func (node *BPNode) deleteItem(key int64) bool {
	index := binarySearchItems(node.leafNode.Items, key)
	if len(node.leafNode.Items) == 0 || node.leafNode.Items[index].key > key {
		//说明当前的key就不存在，删除失败
		return false
	} else if node.leafNode.Items[index].key == key {
		//删除当前的节点
		copy(node.leafNode.Items[index:], node.leafNode.Items[index+1:])          //复制后面的节点到前面去
		node.leafNode.Items = node.leafNode.Items[0 : len(node.leafNode.Items)-1] //将最后一个节点元素删除
		if len(node.leafNode.Items) > 0 {
			node.CommonHeader.maxKey = node.leafNode.Items[len(node.leafNode.Items)-1].key //更新最大值
		}
		return true
	}
	return false
}

func (node *BPNode) deleteChild(child *BPNode) bool {

	index := binarySearchNodes(node.internalNode.Nodes, child.CommonHeader.maxKey) //这个查找的每次都是第一个，由于我们移动，所以要删除的都是当前的右边的节点
	index += 1
	if index < len(node.internalNode.Nodes) && node.internalNode.Nodes[index] == child {
		copy(node.internalNode.Nodes[index:], node.internalNode.Nodes[index+1:])              //在父节点中删除这个孩子节点
		node.internalNode.Nodes = node.internalNode.Nodes[0 : len(node.internalNode.Nodes)-1] //把最后一个节点删除掉
		node.CommonHeader.maxKey = node.internalNode.Nodes[len(node.internalNode.Nodes)-1].CommonHeader.maxKey
		return true
	}
	return false
}

//BPTree B+树的定义
type BPTree struct {
	mutex     sync.Mutex
	root      *BPNode //当前的根节点
	width     int     //当前B+树的阶数,正常的B+树的阶数，再几十到几百之间，
	halfWidth int     //用于[m/2]=ceil(M/2),拆分的时候，一个floor((M+1)/2),一个ceil((M+1)/2)
}

//NewBPTree 初始化B+树
func NewBPTree(width int) *BPTree {
	if width < 3 {
		width = 3
	}
	bt := &BPTree{}
	bt.root = NewLeafNode(width)
	bt.root.CommonHeader.isRoot = true
	bt.width = width
	bt.halfWidth = (bt.width + 1) / 2
	return bt
}

//NewLeafNode 申请width+1是因为插入的时候，可能会暂时出现key大于width的情况，后期再分裂处理
//初始化一个叶子节点
func NewLeafNode(width int) *BPNode {
	leaf := &BPLeafNode{}
	leaf.Items = make([]BPItem, width+1)
	leaf.Items = leaf.Items[0:0] //相当于清空了列表,不是nil
	node := &BPNode{
		CommonHeader: Header{
			isIndex: false,
			isRoot:  false,
		},
		leafNode: leaf,
	}
	return node
}

func newIndexNode(width int) *BPNode {
	internal := &BPInternalNode{}
	internal.Nodes = make([]*BPNode, width+1)
	internal.Nodes = internal.Nodes[0:0] //相当于清空了列表,不是nil
	node := &BPNode{
		CommonHeader: Header{
			isIndex: true,
			isRoot:  false,
		},
		internalNode: internal,
	}
	return node
}

//binarySearchNodes 非叶子节点查找>=key的最小节点,返回所在的节点的索引
func binarySearchNodes(nodes []*BPNode, key int64) int {
	left, right := 0, len(nodes)-1
	for left < right {
		mid := (left + right) / 2
		if key <= nodes[mid].CommonHeader.maxKey {
			right = mid
		} else {
			left = mid + 1
		}
	}
	return left
}

//binarySearchItems 同样查找>=key的最小节点
func binarySearchItems(items []BPItem, key int64) int {
	left, right := 0, len(items)-1
	for left < right {
		mid := (left + right) / 2
		if key <= items[mid].key {
			right = mid
		} else {
			left = mid + 1
		}
	}
	return left
}

//Get B+树的查询
func (t *BPTree) Get(key int64) interface{} {

	t.root.lock.RLock()
	node := t.root //先获得当前的根节点
	for {
		if node.internalNode != nil {
			// 使用二分查找，找到大于key的最小非叶子节点,当前是非叶子节点
			index := binarySearchNodes(node.internalNode.Nodes, key)
			childNode := node.internalNode.Nodes[index] //获得子节点
			childNode.lock.RLock()                      //先把子节点给锁上
			node.lock.RUnlock()                         //解锁父节点
			if childNode == nil {
				return nil
			} else {
				node = childNode
			}
		} else {
			//当前是叶子节点查找元素
			index := binarySearchItems(node.leafNode.Items, key)
			if node.leafNode.Items[index].key == key {
				node.lock.RUnlock() //把当前这个锁给解掉
				return node.leafNode.Items[index].val
			} else {
				node.lock.RUnlock() //把当前这个锁给解掉
				return nil
			}

		}
	}
}

func (t *BPTree) Scan(begin int64, end int64) []interface{} {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	node := t.root //先获得当前的根节点
	for i := 0; node.internalNode != nil && i < len(node.internalNode.Nodes); {
		//遍历当前的根节点，从最小的开始进行查询
		if begin <= node.internalNode.Nodes[i].CommonHeader.maxKey {
			//当前的key小于就可以切换到子节点
			node = node.internalNode.Nodes[i]
			i = 0
			continue
		}
		i++
	}
	//这个走出来的话，就是叶子节点，因为叶子节点的没有子节点
	if node.internalNode != nil {
		//当前没有走到叶子节点
		return nil
	}
	//在当前的叶子节点中查询数据
	var (
		ret []interface{}
	)
	for i := 0; i < len(node.leafNode.Items); {
		if node.leafNode.Items[i].key >= begin && node.leafNode.Items[i].key <= end {
			//找到了起点
			ret = append(ret, node.leafNode.Items[i].val)
		}
		if node.leafNode.Items[i].key > end {
			break
		}
		if i == len(node.leafNode.Items)-1 {
			//到了最后一个元素
			if node.leafNode.Next == nil {
				break
			}
			node = node.leafNode.Next
			i = 0
			continue
		}
		i++

	}
	return ret
}

//Set 往B+树中插入数据
func (t *BPTree) Set(key int64, value interface{}) {
	t.setValue(nil, t.root, key, value)
}

//setValue 递归的实现数据的插入
func (t *BPTree) setValue(parent *BPNode, node *BPNode, key int64, value interface{}) {
	//插入时，首先要先定位到叶子节点,如果是非叶子节点的话，就会直接离开,这个循环
	node.lock.RLock() //进来先加入一个读锁
	for node.internalNode != nil {
		index := binarySearchNodes(node.internalNode.Nodes, key)
		childNode := node.internalNode.Nodes[index]
		//释放当前节点的读锁
		node.lock.RUnlock()
		t.setValue(node, childNode, key, value) //低归的进入到这个函数中进行添加节点
		//递归回来之后，再次得到当前节点的读锁
		node.lock.RLock()
		break
	}
	//定位到叶子节点,叶子节点才要加写锁
	if node.leafNode != nil {
		node.lock.RUnlock() //把当前节点的读锁释放
		node.lock.Lock()    //给当前节点加写锁
		defer node.lock.Unlock()
		//当前低归到了叶子节点,没有子节点
		node.setValue(key, value)
	}
	//节点先插入后分裂，尝试是否会发生节点的分裂,这个地方既可能是非叶子节点，也可能是叶子节点
	var isInternalSplit = node.internalNode != nil && len(node.internalNode.Nodes) > t.width
	var isLeafSplit = node.leafNode != nil && len(node.leafNode.Items) > t.width
	if isInternalSplit || isLeafSplit {
		//因为当前进来已经加了读锁，所以需要改成一个写锁
		if node.internalNode != nil {
			//当前是非叶子节点的情况下处与读锁状态
			node.lock.RUnlock() //把当前节点的读锁释放
			node.lock.Lock()    //给当前节点加写锁
			defer node.lock.Unlock()
		}
		//当前可以被分裂所以就需要进行分裂
		nodeNew := t.splitNode(node, isInternalSplit, isLeafSplit) //
		defer nodeNew.lock.Unlock()                                //把当前的写锁给释放掉
		if parent != nil {
			parent.lock.Lock()
			defer parent.lock.Unlock()
		}
		//当前出现了一个新的节点，说明发生了节点的分裂
		//如果父节点不存在的话，就需要创建一个新的父节点
		//由于当前是出与分裂状态，上面加了一个写锁，所以当前都处与写锁状态
		if parent == nil {
			if node.leafNode != nil && node.CommonHeader.isRoot {
				node.CommonHeader.isRoot = false //改变当前为非根节点
			}
			parent = newIndexNode(t.width)
			parent.lock.Lock() //先把当前加锁
			defer parent.lock.Unlock()
			parent.addChild(node) //把当前的节点添加为子节点
			parent.CommonHeader.isRoot = true
			t.root = parent //更新根节点
		}
		//因为当前节点的父节点一直没有上锁，所以只可能是在当前第一次上锁，或者是创造新父亲的时候上一次写锁
		parent.addChild(nodeNew) //把新节点也添加上
	} else {
		//当前不需要被分裂
		if node.internalNode != nil {
			defer node.lock.RUnlock()
		}
	}

	//如果是叶子节点就释放写锁
	if parent != nil && parent.CommonHeader.maxKey < key { //一层一层往上面低归修改最大值
		parent.CommonHeader.maxKey = key
	}

}

//对节点进行分裂
func (t *BPTree) splitNode(node *BPNode, isInternalSplit, isLeafSplit bool) *BPNode {
	if isInternalSplit {
		//进来就已经是写锁状态
		//如果当前的非叶子节点，他的子节点数超过了width宽度，说明就要创建新节点
		//由于当前进来加的是读锁，改成
		node2 := newIndexNode(t.width) //构造一个新节点
		node2.lock.Lock()
		node2.internalNode.Nodes = append(node2.internalNode.Nodes, node.internalNode.Nodes[t.halfWidth:len(node.internalNode.Nodes)]...) //移动一半的数据到新节点中
		node2.CommonHeader.maxKey = node2.internalNode.Nodes[len(node2.internalNode.Nodes)-1].CommonHeader.maxKey
		//修改原节点的数据
		node.internalNode.Nodes = node.internalNode.Nodes[0:t.halfWidth]
		node.CommonHeader.maxKey = node.internalNode.Nodes[len(node.internalNode.Nodes)-1].CommonHeader.maxKey
		return node2
	} else if isLeafSplit {
		//如果当前是叶子节点，同时当前节点的元素超过了width宽度，就需要增加新的节点
		node2 := NewLeafNode(t.width) //得到一个新节点
		node2.lock.Lock()
		node2.leafNode.Items = append(node2.leafNode.Items, node.leafNode.Items[t.halfWidth:len(node.leafNode.Items)]...) //移动后面一半的节点到node2中
		node2.CommonHeader.maxKey = node2.leafNode.Items[len(node2.leafNode.Items)-1].key                                 //最大值，就是最后一个元素的值了
		//修改原来节点的数据
		//叶子节点连成一个链表
		node2.leafNode.Next = node.leafNode.Next //
		node.leafNode.Next = node2               //指向下一个个节点
		node.leafNode.Items = node.leafNode.Items[0:t.halfWidth]
		node.CommonHeader.maxKey = node.leafNode.Items[len(node.leafNode.Items)-1].key //更新当前节点的最大值
		return node2
	}
	//当前元素不需要进行更新,当前节点加的仍然还是读锁，没有变化
	return nil
}

//Remove 删除key所在的节点元素
func (t *BPTree) Remove(key int64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.deleteItem(nil, t.root, key)
}

func (t *BPTree) deleteItem(parent *BPNode, node *BPNode, key int64) {
	//低归的向下查找节点
	for node.internalNode != nil {
		index := binarySearchNodes(node.internalNode.Nodes, key)
		childNode := node.internalNode.Nodes[index]
		t.deleteItem(node, childNode, key) //低归的进入到这个函数中进行添加节点
		break
	}
	if node.leafNode != nil {
		//当前低归到了叶子节点,没有子节点
		node.deleteItem(key)
		if len(node.leafNode.Items) < t.halfWidth {
			//删除当前的节点后，如果当前的节点的key数量<[m/2],就需要借兄弟的，或者和兄弟合并
			t.itemMoveOrMerge(parent, node)
		}
		//删除当前的节点后，如果当前的节点的key数量>=[m/2],就删除完毕

	} else {
		//当前的非叶子节点
		node.CommonHeader.maxKey = node.internalNode.Nodes[len(node.internalNode.Nodes)-1].CommonHeader.maxKey //更新当前节点的最大值
		if len(node.internalNode.Nodes) < t.halfWidth {
			//当前的节点的
			t.childMoveOrMerge(parent, node)
		}

	}
}

//对叶子节点进行移动或者合并
func (t *BPTree) itemMoveOrMerge(parent *BPNode, node *BPNode) {
	//先获得兄弟节点
	var left *BPNode = nil  //如果左边没有就不需要去得到
	var right *BPNode = nil //如果右边没有数据就不需要去得到
	index := binarySearchNodes(parent.internalNode.Nodes, node.CommonHeader.maxKey)
	if parent.internalNode.Nodes[index] == node {
		if index < len(parent.internalNode.Nodes)-1 {
			right = parent.internalNode.Nodes[index+1]
		}
		if index > 0 {
			left = parent.internalNode.Nodes[index-1]
		}
	}
	//将左边的节点取一个记录移动到被删除的节点
	if left != nil && len(left.leafNode.Items) > t.halfWidth {
		//左边的节点存在，同时删除一个元素不会影响到树的结构
		item := left.leafNode.Items[len(left.leafNode.Items)-1]                        //把最后一个节点取出来
		left.leafNode.Items = left.leafNode.Items[0 : len(left.leafNode.Items)-1]      //删除当前的节点
		left.CommonHeader.maxKey = left.leafNode.Items[len(left.leafNode.Items)-1].key //更新左边节点的最大值
		node.leafNode.Items = append([]BPItem{item}, node.leafNode.Items...)           //左边的节点一定是小于被删除的节点的，所以需要被头插
		return
	}
	//将右侧节点取一个记录到被删除的节点
	if right != nil && len(right.leafNode.Items) > t.halfWidth {
		//右边节点取出一个，一定比被删除的节点要大，所以取出第一个
		item := right.leafNode.Items[0]
		right.leafNode.Items = right.leafNode.Items[1:]         //删除当前的节点
		node.leafNode.Items = append(node.leafNode.Items, item) //右边节点一定是比被删除节点大的，所以要尾插
		node.CommonHeader.maxKey = node.leafNode.Items[len(node.leafNode.Items)-1].key
		return
	}
	//合并都是向左合并
	//与左侧节点进行合并,把当前这个节点合并到左侧节点来
	if left != nil && len(left.leafNode.Items)+len(node.leafNode.Items) < t.width {
		left.leafNode.Items = append(left.leafNode.Items, node.leafNode.Items...)
		left.leafNode.Next = node.leafNode.Next
		left.CommonHeader.maxKey = left.leafNode.Items[len(left.leafNode.Items)-1].key
		//在父节点中删除当前的这个节点
		parent.deleteChild(node)
		return
	}
	//与右侧节点进行合并，把右侧节点移动到当前这个被删除的节点来
	if right != nil && len(right.leafNode.Items)+len(node.leafNode.Items) < t.width {
		node.leafNode.Items = append(node.leafNode.Items, right.leafNode.Items...) //把右边节点合并到当前的节点
		//把节点进行删除
		node.leafNode.Next = right.leafNode.Next
		node.CommonHeader.maxKey = node.leafNode.Items[len(node.leafNode.Items)-1].key
		parent.deleteChild(right)
		return
	}

}

func (t *BPTree) childMoveOrMerge(parent *BPNode, node *BPNode) {
	if parent == nil {
		return
	}
	var left *BPNode = nil  //如果左边没有就不需要去得到
	var right *BPNode = nil //如果右边没有数据就不需要去得到
	index := binarySearchNodes(parent.internalNode.Nodes, node.CommonHeader.maxKey)
	if parent.internalNode.Nodes[index] == node {
		if index < len(parent.internalNode.Nodes)-1 {
			right = parent.internalNode.Nodes[index+1]
		}
		if index > 0 {
			left = parent.internalNode.Nodes[index-1]
		}
	}

	//将左边的节点取一个记录移动到被删除的节点
	if left != nil && len(left.internalNode.Nodes) > t.halfWidth {
		//左边的节点存在，同时删除一个元素不会影响到树的结构
		item := left.internalNode.Nodes[len(left.internalNode.Nodes)-1]                       //把最后一个节点取出来
		left.internalNode.Nodes = left.internalNode.Nodes[0 : len(left.internalNode.Nodes)-1] //删除当前的节点
		node.internalNode.Nodes = append([]*BPNode{item}, node.internalNode.Nodes...)         //左边的节点一定是小于被删除的节点的，所以需要被头插
		left.CommonHeader.maxKey = left.internalNode.Nodes[len(left.internalNode.Nodes)-1].CommonHeader.maxKey
		return
	}
	//将右侧节点取一个记录到被删除的节点
	if right != nil && len(right.internalNode.Nodes) > t.halfWidth {
		//右边节点取出一个，一定比被删除的节点要大，所以取出第一个
		item := right.internalNode.Nodes[0]                             //获得右节点的第一个节点元素
		right.internalNode.Nodes = right.internalNode.Nodes[1:]         //删除当前的节点
		node.internalNode.Nodes = append(node.internalNode.Nodes, item) //右边节点一定是比被删除节点大的，所以要尾插
		node.CommonHeader.maxKey = node.internalNode.Nodes[len(node.internalNode.Nodes)-1].CommonHeader.maxKey
		return
	}
	//与左侧节点进行合并
	if left != nil && len(left.internalNode.Nodes)+len(node.internalNode.Nodes) < t.width {
		left.internalNode.Nodes = append(left.internalNode.Nodes, node.internalNode.Nodes...)
		left.CommonHeader.maxKey = left.internalNode.Nodes[len(left.internalNode.Nodes)-1].CommonHeader.maxKey
		//在父节点中删除当前的这个节点
		parent.deleteChild(node)
		return
	}
	//与右侧节点进行合并
	if right != nil && len(right.internalNode.Nodes)+len(node.internalNode.Nodes) < t.width {
		node.internalNode.Nodes = append(node.internalNode.Nodes, right.internalNode.Nodes...) //把右边节点合并到当前的节点
		//把节点进行删除
		node.CommonHeader.maxKey = node.internalNode.Nodes[len(node.internalNode.Nodes)-1].CommonHeader.maxKey
		parent.deleteChild(right)
		return
	}

}
