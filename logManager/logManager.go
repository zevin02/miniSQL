package logManager

import (
	fm "miniSQL/file_manager"
	"sync"
)

//日志都是按照倒装的形式来进行存储，假设缓存区中有400字节，有3条日志，第一条100字节（存在300-400的位置），第二条50字节（250-300），第三条100字节（150-250）
//这样因为我们是从开头往后读取的，所以这样我们就会先读取到最新的日志

const (
	UINT64_LEN = 8
)

//LogManager 日志管理器
type LogManager struct {
	fileManager  *fm.FileManager //文件管理器
	logFile      string          //日志文件的名称
	logPage      *fm.Page        //存储日志的缓冲区,固定就只有一个缓冲块，不断的重复利用,(当前缓冲区的头8字节存储下一次开始写入的位置的末尾位置)
	currentBlk   *fm.BlockId     //日志当前写入(正在处理)的区块号
	lastestLsn   uint64          //当前最新日志的编号  log Sequence Number(还没有刷新的磁盘中)
	lastSavedLsg uint64          //上一次(刷新)写入磁盘的日志编号
	mu           sync.RWMutex
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
	l.logPage.SetInt(0, uint64(l.fileManager.BlockSize())) //BlockSize==>400，往缓冲区中写入8字节的400,下一次再写入的时候，就需要先读取8字节，查看从哪个地方开始写入
	l.fileManager.Write(&blk, l.logPage)                   //把当前日志缓冲区的内容，写入到日志文件中
	return &blk, nil
}

func NewLogManager(fileManager *fm.FileManager, logFile string) (*LogManager, error) {
	logMgr := LogManager{
		fileManager:  fileManager,
		logFile:      logFile,
		logPage:      fm.NewPageBySize(fileManager.BlockSize()), //开辟一个新的缓冲区
		lastSavedLsg: 0,                                         //当前还没有写入日志，所以就是0
		lastestLsn:   0,                                         //最新添加的日志号也是0
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
		logMgr.currentBlk = blk
	} else {
		//文件已经存在，就需要先把末尾的日志内容先读取到内存中，如果当前对应的区块还有空间，新的日志就得继续写入当前的区块
		logMgr.currentBlk = fm.NewBlockId(logMgr.logFile, logSize-1)
		logMgr.fileManager.Read(logMgr.currentBlk, logMgr.logPage)
	}
	return &logMgr, nil
}

//FlushByLSN LSN- log sequence number
func (l *LogManager) FlushByLSN(lsn uint64) error {
	//把给定编号及其之前的日志写入到磁盘
	//当我们写入给定编号的日志的时候，接口会把同当前日志处与同一区块的日志写入到磁盘中，假设当前的日志是65,
	//如果66,67,68也处与同一个区块中，那么他们也会写入到磁盘中
	if lsn > l.lastSavedLsg {
		//当前的日志编号，比上一次写入到缓冲区中的日志编号要大，就需要进行刷新
		err := l.Flush()
		if err != nil {
			return err
		}
		//更新写入的编号
		l.lastestLsn = lsn
	}
	return nil
}

//Flush 将缓冲区中的数据刷新到磁盘中
func (l *LogManager) Flush() error {
	//把给定缓冲区的数据写入到磁盘中
	_, err := l.fileManager.Write(l.currentBlk, l.logPage)
	if err != nil {
		return err
	}
	return nil
}

//Append 写入一条新的数据到缓冲区中，并返回当前最新的日志的编号
func (l *LogManager) Append(logRecord []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	//先读取缓冲区的头8个字节，获取可以写入的偏移
	boundary := l.logPage.GetInt(0) //得到可以写入的偏移
	logRecordSize := uint64(len(logRecord))
	bytesNeed := logRecordSize + UINT64_LEN //这个是实际需要写入到缓冲区中占用的大小
	if int(boundary-bytesNeed) < int(UINT64_LEN) {
		//因为前8个字节是存储下一次要开始的边界
		//所以当前没有足够的空间了,先将当前的缓冲区中存储的数据写入到磁盘中，再分配新的区块空间
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
	recordPos := boundary - bytesNeed //这样就到了头部了,得到了写入的偏移
	l.logPage.SetBytes(recordPos, logRecord)
	l.logPage.SetInt(0, recordPos) //重新设置可以写入的偏移
	//因为我们成功的写入了一个新的日志，所以把成功写入的日志号+1
	l.lastestLsn += 1
	return l.lastestLsn, nil
}

//获得日志文件的迭代器，进行遍历日志文件的内容
func (l LogManager) Iterator() *LogIterator {
	l.Flush() //先将缓冲区中的数据刷新到磁盘中
	return NewLogIterator(l.fileManager, l.currentBlk)
}
