package file_manager

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

//FileManager 文件系统的实现对象
type FileManager struct {
	DirPath   string              //所有数据都存储再一个给定的数据目录的路径
	blockSize uint64              //一个区块的大小
	isNew     bool                //用来判断DirPath路径是否存在，如果存在就时false，不存在就是true
	openFiles map[string]*os.File //打开的文件的文件句柄
	mu        sync.RWMutex
}

//NewFileManager 初始化一个文件管理器
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
func (f FileManager) Read(blk *BlockId, p *Page) (int, error) {
	//blockID指定了读取的哪个文件的哪个区块
	f.mu.RLock()
	defer f.mu.RUnlock()
	file, err := f.getFile(blk.FileName()) //先从BlockId中获得要读取的文件名,并打开相应的文件
	if err != nil {
		return 0, err
	}
	defer file.Close() //读取完就将当前文件进行关闭
	//把数据放到page的缓冲区中,从文件在磁盘存储的某个区块开始进行读取
	//int64(blk.Number()*f.BlockSize),二进制文件的偏移的位置，受他存储的第几个区块决定的
	cout, err := file.ReadAt(p.Contents(), int64(blk.Number()*f.blockSize)) //
	if err != nil {
		return 0, err
	}
	return cout, nil
}

//blockId记录了要读写的文件名和位置，把page中的数据进行写入
func (f FileManager) Write(blk *BlockId, p *Page) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, err := f.getFile(blk.FileName()) //先从BlockId中获得要读取的文件名,并打开相应的文件
	if err != nil {
		return 0, err
	}
	defer file.Close() //读取完就将当前文件进行关闭
	//在给定的位置进行写入
	cout, err := file.WriteAt(p.Contents(), int64(blk.Number()*f.blockSize))
	if err != nil {
		return 0, err
	}
	return cout, nil
}

//size 返回文件的大小(占用了多少个区块数量)
func (f FileManager) Size(fileName string) (uint64, error) {
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

//Append 给当前的文件再增加一个区块(扩大文件的大小)
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

func (f FileManager) BlockSize() uint64 {
	return f.blockSize
}
