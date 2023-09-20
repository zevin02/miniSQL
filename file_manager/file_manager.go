package file_manager

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

/*
	有4个因素回影响磁盘的读写速度：
	1. 容量（磁盘能够存储的数据量），现在一个盘片的容量可以达到40G，容量越大（磁盘的数据密度就会增加：意味者相同物理空间中可以存储更多的数据，读写头需要在更小的区域内进行操作），读写效率越满
	2. 旋转速度（磁盘旋转一周需要的时间）通常磁盘一分钟能旋转5400转/15000转
	3. 传输数度(数据被磁头最后送到内存花费的时间)
	4. 磁头挪动时间（磁头从当前磁道挪动到目标轨道的时间）


	因为磁头移动的时候，把所有磁头同时移动到给定的轨道：所以我们就可以把同一个文件的数据写入到不同盘面的同一个轨道中，如果当前的柱面数据都写完了
	可以将数据存放到相邻的轨道中(磁头移动的距离缩短)

	我们把文件的当作磁盘看待，按照区块为单位来对文件进行读写，OS会尽量把同一个文件的数据存储在磁盘的同一轨道中，或是距离近的轨道中
	我们把若干扇区当作作为一个统一单元来进行读写Block
*/

//FileManager 文件系统的实现对象
type FileManager struct {
	DirPath   string              //所有数据都存储再一个给定的数据目录的路径
	blockSize uint64              //一个区块的大小
	isNew     bool                //用来判断DirPath路径是否存在，如果存在就时false，不存在就是true
	openFiles map[string]*os.File //打开的文件的文件句柄
	mu        sync.RWMutex
}

//NewFileManager 初始化一个文件管理器,传入数据目录的路径,以及一个block块的大小
func NewFileManager(dirPath string, blockSize uint64) (*FileManager, error) {
	fileManager := &FileManager{
		DirPath:   dirPath,
		blockSize: blockSize,
		isNew:     false,
		openFiles: make(map[string]*os.File),
	}

	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		//如果当前的目录不存在，就要先进行创建
		fileManager.isNew = true //new说明再new的时候创建了一个新的目录
		if err := os.Mkdir(dirPath, os.ModePerm); err != nil {
			//创建一个新的目录
			return nil, err
		}
	} else {
		//如果目录已经存在，就需要把目录下的临时文件都删除掉
		err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
			mode := info.Mode()
			if mode.IsRegular() {
				//如果时普通文件，检查他的前缀是否是临时文件，如果是的话，就需要进行将这个临时文件进行删除
				name := info.Name()
				if strings.HasPrefix(name, "temp") {
					//发现当前是一个临时文件，所以就需要将当前这个临时文件进行删除
					os.Remove(filepath.Join(path, name))
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return fileManager, nil
}

//getFile 打开相应的文件，获得对应的句柄
func (f *FileManager) getFile(fileName string) (*os.File, error) {
	path := filepath.Join(f.DirPath, fileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644) //打开一个文件
	if err != nil {
		return nil, err
	}
	f.openFiles[path] = file //将打开的文件句柄进行保存
	return file, nil
}

//Read 根据BlockId指定的磁盘的某个区块来进行读取数据
//把数据读取到page中
func (f *FileManager) Read(blk *BlockId, p *Page) (int, error) {
	//blockID指定了读取的哪个文件的哪个区块
	f.mu.RLock()
	defer f.mu.RUnlock()
	file, err := f.getFile(blk.FileName()) //先从BlockId中获得要读取的文件名,并打开相应的文件
	if err != nil {
		return 0, err
	}
	//todo file close 后需要把他的句柄从打开的文件map中去掉
	defer file.Close() //读取完就将当前文件进行关闭
	//把数据放到page的缓冲区中,从文件在磁盘存储的某个区块开始进行读取
	//int64(blk.Number()*f.BlockSize),二进制文件的偏移的位置，受他存储的第几个区块决定的
	//读取出来一个block块大小的数据
	cout, err := file.ReadAt(p.Contents(), int64(blk.Number()*f.blockSize)) //
	if err != nil {
		return 0, err
	}
	return cout, nil
}

//Write blockId记录了要读写的文件名和位置，把page中的数据进行写入
func (f *FileManager) Write(blk *BlockId, p *Page) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, err := f.getFile(blk.FileName()) //先从BlockId中获得要读取的文件名,并打开相应的文件
	if err != nil {
		return 0, err
	}
	defer file.Close() //读取完就将当前文件进行关闭
	//在给定的位置进行写入,把缓冲区的数据写入到磁盘
	//id=1,说明从400开始写入
	count, err := file.WriteAt(p.Contents(), int64(blk.Number()*f.blockSize))
	if err != nil {
		return 0, err
	}
	return count, nil
}

//Size 返回文件的大小(占用了多少个区块数量)
func (f *FileManager) Size(fileName string) (uint64, error) {
	file, err := f.getFile(fileName)
	if err != nil {
		return 0, err
	}
	defer file.Close() //处理完就将当前文件进行关闭
	fi, err := file.Stat()
	if err != nil {
		return 0, err
	}
	//返回当前文件占用了多少个区块
	return uint64(fi.Size()) / f.blockSize, nil
}

//Append 给当前的文件再增加一个区块(扩大文件的大小)(当前文件写满了才会继续调用这个append进行写入)
func (f *FileManager) Append(fileName string) (BlockId, error) {
	newBlockNum, err := f.Size(fileName) //获得当前的文件的区块id
	if err != nil {
		return BlockId{}, err
	}
	blk := NewBlockId(fileName, newBlockNum) //新增加一个区块
	file, err := f.getFile(blk.fileName)     //打开当前的文件
	if err != nil {
		return BlockId{}, err
	}
	defer file.Close()
	b := make([]byte, f.blockSize)                            //构造一个空的数组
	_, err = file.WriteAt(b, int64(blk.Number()*f.blockSize)) //将当前的空数组写入到文件中，标识文件扩大了
	if err != nil {
		return BlockId{}, err
	}
	return *blk, nil
}

func (f *FileManager) IsNew() bool {
	return f.isNew
}

func (f *FileManager) BlockSize() uint64 {
	return f.blockSize
}
