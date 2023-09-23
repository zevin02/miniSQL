package logManager

import (
	fm "miniSQL/file_manager"
	"sync"
)

/*
	日志通常是用来记录系统的运行状态，类似快照，如果系统出现了问题，管理员或者代码本身就可以扫描分析日志来确定问题所在，或者通过日志执行错误恢复
	通过使用日志来保证数据的一致性
	数据库在读写数据前，需要先写入日志记录相应的操作，例如当前操作是读还是写，然后记录要写入的数据
	例如：我们有业务，要把100行数据写入到两个表中，前50行写入表1,后50行写入表2
	就会出现类似这样的信息“表1写入0-50行”，“表2写入51-100行”
	假设数据在写入前50行后突然断电，机器重启的时候，就会自动扫描日志发现“表2写入51-100行”这个操作没有执行，于是就再次执行这个操作，这样就保证了数据的一致性

*/
/*
	数据采用“压栈”的方式
	日志都是按照倒装的形式来进行存储，假设缓存区中有400字节，有3条日志，第一条100字节（存在300-400的位置），第二条50字节（250-300），第三条100字节（150-250）
	这样因为我们是从开头往后读取的，所以这样我们就会先读取到最新的日志jian

	日志的落盘时间
	1.当当前的缓存满了，新开辟一块新的区块的时候就会将当前数据落盘
	2.当获得迭代器的时候，就会将当前数据落盘
	3.手动的调用Flush
*/

const (
	UINT64_LEN = 8
)

//LogManager 日志管理器
type LogManager struct {
	fileManager       *fm.FileManager //文件管理器,用来管理日志文件的读写
	logFile           string          //日志文件的名称
	logPage           *fm.Page        //存储日志的缓冲区,固定就只有一个缓冲块，不断的重复利用,(当前缓冲区的头8字节存储下一次开始写入的位置的末尾位置)，根据固定的策略将这个页进行落盘
	currentBlk        *fm.BlockId     //日志当前写入(正在处理)的区块号
	lastestLsn        uint64          //当前最新日志的编号  log Sequence Number(还没有刷新的磁盘中)
	lastSaved2DiskLsn uint64          //上一次(刷新)写入磁盘的日志编号
	mu                sync.RWMutex
}

//appendNewBlock 创建一个新的Block来方便后续的写入日志
func (l *LogManager) appendNewBlock() (*fm.BlockId, error) {
	//当缓冲区用完了调用该接口来分配新的内存
	blk, err := l.fileManager.Append(l.logFile) //再日志二进制文件末尾添加一个区块,扩大了该文件可以管理的空间大小
	if err != nil {
		return nil, err
	}
	/*
		添加日志的时候是从内存的底部往上写入的，缓冲区400字节，日志100字节，就会写入到300-400的位置中，首先再缓冲区的头8字节写入偏移
		假设日志100字节写入缓冲区，下一次写入的偏移位置就要从300开始算起，300就需要写入缓冲区中头8字节
	*/
	l.logPage.SetInt(0, int64(l.fileManager.BlockSize())) //BlockSize==>400，往缓冲区中写入8字节的400,下一次再写入的时候，就需要先读取8字节，查看从哪个地方开始写入
	l.fileManager.Write(&blk, l.logPage)                  //把当前日志缓冲区的内容，写入到日志文件中
	return &blk, nil
}

//NewLogManager 当前日志管理器在打开的时候，如果没有文件中没有数据的话就创建了一个区块并写入了一个区块大小的数据，如果有数据了，就继续使用最新的区块，直到填满数据
func NewLogManager(fileManager *fm.FileManager, logFile string) (*LogManager, error) {
	logMgr := LogManager{
		fileManager:       fileManager,
		logFile:           logFile,
		logPage:           fm.NewPageBySize(fileManager.BlockSize()), //开辟一个新的缓冲区,后面就循环利用这个缓冲区
		lastSaved2DiskLsn: 0,                                         //当前还没有写入日志，所以就是0
		lastestLsn:        0,                                         //最新添加的日志号也是0
	}
	logSize, err := fileManager.Size(logFile) //获得当前的日志文件的ID（从0开始的下标）
	if err != nil {
		return nil, err
	}
	if logSize == 0 {
		//当前的文件是空，就需要位当前文件添加一个新的区块
		blk, err := logMgr.appendNewBlock()
		if err != nil {
			return nil, err
		}
		logMgr.currentBlk = blk //更新当前正在操纵的区块
	} else {
		//文件已经存在，就需要先把末尾的日志内容先读取到内存中，如果当前对应的区块还有空间，新的日志就得继续写入当前的区块,当前size=400,所以我们就需要打开0号块，在有数据增加进来的话，才会新开辟一个数据块
		logMgr.currentBlk = fm.NewBlockId(logMgr.logFile, logSize-1)
		logMgr.fileManager.Read(logMgr.currentBlk, logMgr.logPage) //把数据读取上来
	}
	return &logMgr, nil
}

//FlushByLSN LSN- log sequence number,刷新日志
func (l *LogManager) FlushByLSN(lsn uint64) error {
	//把给定编号及其之前的日志写入到磁盘
	//当我们写入给定编号的日志的时候，接口会把同当前日志处与同一区块的日志写入到磁盘中，假设当前的日志是65,
	//如果66,67,68也处与同一个区块中，那么他们也会写入到磁盘中
	if lsn > l.lastSaved2DiskLsn {
		//当前的日志编号，比上一次写入到缓冲区中的日志编号要大，就需要进行刷新
		err := l.Flush()
		if err != nil {
			return err
		}
		//更新写入的编号
		l.lastestLsn = lsn
		l.lastSaved2DiskLsn = l.lastestLsn //同样更新上次刷新到磁盘的日志号
	}
	return nil
}

//Flush 实际将日志缓冲区中的数据刷新到磁盘中
func (l *LogManager) Flush() error {
	//把给定缓冲区的数据写入到磁盘中
	_, err := l.fileManager.Write(l.currentBlk, l.logPage)
	if err != nil {
		return err
	}
	return nil
}

//Append 写入一条新的日志数据到日志缓冲区中(还没有刷新到磁盘中)，并返回当前最新的日志的编号
func (l *LogManager) Append(logRecord []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	//先读取缓冲区的头8个字节(在创建一个新的blockid的时候，就会在page的头部写入下一次数据的截至位置)，获取可以写入的偏移
	boundary := l.logPage.GetInt(0) //得到可以此次可写入的偏移
	logRecordSize := int64(len(logRecord))
	bytesNeed := logRecordSize + UINT64_LEN //这个是实际需要写入到缓冲区中占用的大小,+8是代表这个数据的长度
	if int(boundary-bytesNeed) < int(UINT64_LEN) {
		//因为前8个字节是存储下一次要开始的边界
		//所以当前缓冲区已经被写满了，没有足够的空间了,就可以将当前的缓冲区中存储的数据写入到磁盘中，再分配新的区块空间，继续写入
		err := l.Flush()
		if err != nil {
			return l.lastestLsn, err
		}
		//分配新的空间用于写入新的数据
		l.currentBlk, err = l.appendNewBlock() //分配一个新的区块去装载二进制数组的内容
		if err != nil {
			return l.lastestLsn, err
		}
		boundary = l.logPage.GetInt(0) //更新获得当前可以写入的最末尾的偏移
	}
	recordPos := boundary - bytesNeed                //这样就到了头部了,得到了可以写入的起始偏移
	l.logPage.SetBytes(uint64(recordPos), logRecord) //往日志的缓冲区中写入当前的日志信息
	l.logPage.SetInt(0, recordPos)                   //重新设置可以写入的偏移,供下一次写入的时候读取
	//因为我们成功的写入了一个新的日志，所以把成功写入的日志号+1
	l.lastestLsn += 1
	return l.lastestLsn, nil
}

//Iterator 获得日志文件的迭代器，进行遍历日志文件的内容
func (l *LogManager) Iterator() *LogIterator {
	l.Flush()                                          //获得迭代器的时候先将缓冲区中的数据刷新到磁盘中,保证数据完全落盘
	return NewLogIterator(l.fileManager, l.currentBlk) //从最后一个数据块开始读取，往前遍历
}
