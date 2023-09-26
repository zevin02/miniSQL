package metadata_manager

import (
	rm "miniSQL/record_manager"
	tx "miniSQL/transaction"
	"sync"
)

/*
	数据库系统需要维护统计信息
	1.每个表使用了多少个区块
	2.每个表中包含了多少条的记录
	3.对于某个表中的某个字段，有多少个不重复的值

	维护这些统计信息需要有性能代价，数据库启动的时候，会扫描所有的数据库表，构建出来统计信息，并将这些信息保留在内存中，并定时更新
	由于是定时更新，而不是实时更新，所以有可能在某个时刻统计信息和实际情况会不相同.但是这样并不会有太大的影响，因为这些数据只是用来做估计查询成本，而不会影响查询结果

*/

const (
	REFRESH_STAT_INFO_COUNT = 100 //数据库表发生变化100次后更新统计数据
)

//StatInfo 一张表的状态信息
type StatInfo struct {
	numBlock int //数据库表的区块数量
	numRecs  int //数据库表中包含的记录数
}

func NewStatInfo(numBlock int, numRecs int) *StatInfo {
	return &StatInfo{
		numRecs:  numRecs,
		numBlock: numBlock,
	}
}

//BLockAccessed 当前表访问了多少个区块
func (s *StatInfo) BLockAccessed() int {
	return s.numBlock
}

//RecordsOutput 当前表记录了多少条的数据
func (s *StatInfo) RecordsOutput() int {
	return s.numRecs
}

//DistinctValue 返回当前表中的某个字段有多少个不同的值
//todo 进一步优化，使用哈希表或者临时表来进行实现
func (s *StatInfo) DistinctValue(fldName string) int {
	return 1 + (s.numRecs / 3)
}

//StatManager 状态管理器，管理当前数据库的状态,他只在系统启动的时候创建，在创建的时候，会调用refreshStatistics来创建统计数据并存储在内存中

type StatManager struct {
	tblgr      *TableManager
	tableStats map[string]*StatInfo //管理每张表的信息
	numCalls   int                  //调用当前StatManager的次数
	lock       sync.Mutex
}

//NewStatManager 初始化一个状态管理器
func NewStatManager(tblgr *TableManager, tx *tx.Transaction) (*StatManager, error) {
	statmgr := &StatManager{
		tblgr:    tblgr,
		numCalls: 0,
	}
	err := statmgr.refreshStatistics(tx) //遍历每张表，获得统计信息
	if err != nil {
		return nil, err
	}
	return statmgr, nil
}

//refreshStatistics 获得每张表的元数据,统计信息
func (s *StatManager) refreshStatistics(tx *tx.Transaction) error {
	s.tableStats = make(map[string]*StatInfo)
	s.numCalls = 0
	layout, _ := s.tblgr.GetLayout("tblcat", tx)     //获得tblcat表，通过这张表，可以得到每张表的名字
	tcat, _ := rm.NewTableScan(tx, "tblcat", layout) //对tblcat表进行读写操作
	for tcat.Next() {
		tblName := tcat.GetString("tblname")             //获得表名
		layout, _ := s.tblgr.GetLayout(tblName, tx)      //通过每张表的名字，获得对应的表结构
		si, err := s.calcTableStats(tblName, layout, tx) //计算当前表的统计信息
		if err != nil {
			return err
		}
		s.tableStats[tblName] = si
	}
	return nil
}

//calcTableStats 遍历当前的表，统计得到当前表的统计信息,TODO 后期更新成某一个时刻的快照扫描
func (s *StatManager) calcTableStats(tblname string, layout *rm.Layout, tx *tx.Transaction) (*StatInfo, error) {
	numRecord := 0
	numBlock := 0
	ts, err := rm.NewTableScan(tx, tblname, layout) //获得当前表的数据
	if err != nil {
		return nil, err
	}
	defer ts.Close()
	for ts.Next() {
		//遍历当前表，并填充相应的元数据
		numRecord += 1
		numBlock = ts.GetRid().BlockNumber() + 1 //通过rid可以获得当前处在哪个区块的哪个slot中
	}
	return NewStatInfo(numBlock, numRecord), nil

}

//GetStatInfo 获得某张表的统计数据，如果没有存在的话，就需要创建
func (s *StatManager) GetStatInfo(tblname string, layout *rm.Layout, tx *tx.Transaction) (*StatInfo, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.numCalls += 1
	var err error
	if s.numCalls > REFRESH_STAT_INFO_COUNT {
		//如果调用当前超过了可刷新的次数，我们就需要更新元数据
		err = s.refreshStatistics(tx)
		if err != nil {
			return nil, err
		} //重新计算统计数据
		s.numCalls = 0 //更新完之后，重新设置成0,等待后续更新
	}
	si, ok := s.tableStats[tblname] //获得某张表的统计信息
	if !ok {
		//当前表不存在,为当前数据库表创建统计对象
		si, err = s.calcTableStats(tblname, layout, tx)
		if err != nil {
			return nil, err
		}
	}
	return si, nil
}
