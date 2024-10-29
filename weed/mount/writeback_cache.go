package mount

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/page_writer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	writeBufferSize  = 512 * 1024
	defaultFileCount = 4000
	cacheExpireTime  = 1 * time.Minute
)

type WritebackCache struct {
	sync.Mutex
	lruList   *lru.Cache[string, *TempFileHandle]
	chunkSize int64
	quotaSize int64
	cacheDir  string
	wfs       *WFS
}

type TempFileHandle struct {
	path           string
	file           *os.File
	size           int64
	timeStamp      time.Time
	uploadPipeline *page_writer.UploadPipeline
	dirtyPages     *ChunkedDirtyPages
}

func NewWritebackCache(chunkSize int64, wfs *WFS, quotaLimitMB int64) *WritebackCache {
	cache, _ := lru.New[string, *TempFileHandle](defaultFileCount)

	return &WritebackCache{
		lruList:   cache,
		chunkSize: chunkSize,
		quotaSize: quotaLimitMB << 20,
		cacheDir:  wfs.option.CacheDirForWrite,
		wfs:       wfs,
	}
}

func (c *WritebackCache) OpenFile(filePath string, fh *FileHandle) error {
	c.Lock()
	defer c.Unlock()

	fileFullPath := util.FullPath(filePath)
	fileDir, fileName := fileFullPath.DirAndName()
	tempFileDir := filepath.Join(c.cacheDir, fileDir)
	glog.V(4).Infof("create temp file dir %s", tempFileDir)

	if _, err := os.Stat(tempFileDir); os.IsNotExist(err) {
		if err := os.MkdirAll(tempFileDir, 0755); err != nil {
			return err
		}
	} else if err != nil {
		glog.V(4).Infof("stat temp dir error: %v", err)
		return err
	}

	tempFilePath := filepath.Join(tempFileDir, fileName)
	glog.V(4).Infof("open temp file %s at %s", filePath, tempFilePath)

	if _, ok := c.lruList.Peek(tempFilePath); ok {
		return nil
	}

	f, err := os.OpenFile(tempFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		glog.V(4).Infof("create temp file error: %v", err)
		return err
	}

	fileInfo, err := f.Stat()
	if err != nil {
		return err
	}

	dirtyPages := &ChunkedDirtyPages{
		fh: fh,
	}
	uploadPipeline := page_writer.NewUploadPipeline(fh.wfs.concurrentWriters, c.chunkSize,
		dirtyPages.saveChunkedFileIntervalToStorage, fh.wfs.option.ConcurrentWriters, "")

	glog.V(4).Infof("add temp file %s for %s, size: %d", filePath, tempFilePath, fileInfo.Size())
	c.lruList.Add(filePath, &TempFileHandle{
		path:           tempFilePath,
		file:           f,
		size:           fileInfo.Size(),
		uploadPipeline: uploadPipeline,
		dirtyPages:     dirtyPages,
	})

	return nil
}

func (c *WritebackCache) WriteChunkToFile(path string, reader io.Reader, offset, size int64) (int64, error) {
	c.Lock()
	defer c.Unlock()

	glog.V(4).Infof("write chunk to file %s, offset: %d, size: %d", path, offset, size)

	fh, ok := c.lruList.Get(path)
	if !ok {
		return 0, os.ErrNotExist
	}
	var data []byte
	var err error

	bytesReader, ok := reader.(*util.BytesReader)
	if ok {
		data = bytesReader.Bytes
	} else {
		data, err = io.ReadAll(reader)
		if err != nil {
			err = fmt.Errorf("read input: %v", err)
			return 0, err
		}
	}
	written, err := fh.file.WriteAt(data, offset)
	if err != nil {
		return int64(written), err
	}
	fh.file.Sync()

	fileInfo, err := fh.file.Stat()
	if err != nil {
		return 0, err
	}
	glog.V(4).Infof("update temp file %s, size: %d", fh.path, fileInfo.Size())
	curSize := int64(fileInfo.Size())
	if fh.size != curSize {
		fh.size = curSize
	}
	fh.timeStamp = time.Now()
	c.lruList.Add(path, fh)

	return int64(written), nil
}

func (c *WritebackCache) LoadTempFiles() error {
	rootDir := c.cacheDir
	if _, err := os.Stat(rootDir); os.IsNotExist(err) {
		return nil
	}
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			glog.V(4).Infof("open temp file error: %v", err)
			return err
		}
		fileInfo, err := f.Stat()
		if err != nil {
			return err
		}
		filePath := strings.TrimPrefix(path, c.cacheDir)
		var fh *FileHandle
		var fhFound bool
		fullFilePath := util.FullPath(filePath)
		inode, inodeFound := c.wfs.inodeToPath.GetInode(fullFilePath)
		if !inodeFound {
			glog.V(4).Infoln("inode not found for file", filePath)
			return nil
		}
		fh, fhFound = c.wfs.fhMap.FindFileHandle(inode)
		if !fhFound {
			glog.V(4).Infoln("file handle not found for inode", inode)
			return nil
		}
		dirtyPages := &ChunkedDirtyPages{
			fh: fh,
		}
		uploadPipeline := page_writer.NewUploadPipeline(c.wfs.concurrentWriters, c.chunkSize,
			dirtyPages.saveChunkedFileIntervalToStorage, c.wfs.option.ConcurrentWriters, "")

		glog.V(4).Infof("add file %s, temp file: %s, size: %d", filePath, path, fileInfo.Size())
		c.lruList.Add(filePath, &TempFileHandle{
			path:           path,
			file:           f,
			size:           fileInfo.Size(),
			uploadPipeline: uploadPipeline,
			dirtyPages:     dirtyPages,
		})
		return nil
	})

	if err != nil {
		glog.V(4).Infof("load temp files error: %v", err)
		return err
	}

	return nil
}

// upload and remove temp file
func (c *WritebackCache) doFlush(filePath string, fh *TempFileHandle) {
	chunks := tempFileToMemChunks(fh.file, c.chunkSize)
	if len(chunks) == 0 {
		return
	}
	glog.V(4).Infoln("uploading temp file, chunks", filePath, len(chunks))

	offset := int64(0)
	for _, chunk := range chunks {
		fh.uploadPipeline.AddChunk(chunk, offset)
		offset += chunk.WrittenSize()
	}
	fh.uploadPipeline.FlushAll()

	fh.dirtyPages.Destroy()
	c.lruList.Remove(filePath)
	fh.file.Close()
	os.Remove(filePath)
}

func (c *WritebackCache) Run(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			glog.V(4).Infoln("start cache size check")
			expiredList := make([]*TempFileHandle, 0)
			for _, fh := range c.lruList.Values() {
				if fh.timeStamp.Add(cacheExpireTime).Before(time.Now()) {
					expiredList = append(expiredList, fh)
				}
			}
			glog.V(4).Infoln("process expired files", len(expiredList))
			for _, fh := range expiredList {
				c.doFlush(fh.path, fh)
			}
			for {
				if !c.checkCacheQuota() {
					break
				}
				filePath, fh, ok := c.lruList.GetOldest()
				if !ok {
					continue
				}
				c.doFlush(filePath, fh)
			}
		}
	}
}

func (c *WritebackCache) checkCacheQuota() bool {
	c.Lock()
	defer c.Unlock()

	totalSize := int64(0)
	for _, fh := range c.lruList.Values() {
		totalSize += fh.size
	}
	if totalSize >= c.quotaSize {
		glog.V(4).Infoln("cache size exceeds quota, total size:", totalSize, "quota size:", c.quotaSize)
		return true
	}
	return false
}

func tempFileToMemChunks(file *os.File, chunkSize int64) []*page_writer.MemChunk {
	chunks := make([]*page_writer.MemChunk, 0)

	fileInfo, err := file.Stat()
	if err != nil {
		glog.V(4).Infoln("stat temp file error: ", err)
		return nil
	}
	size := int64(fileInfo.Size())
	numChunks := int(size / chunkSize)

	if size <= chunkSize {
		buf := make([]byte, size)
		_, err := file.Read(buf)
		if err != nil {
			return nil
		}
		chunk := page_writer.NewMemChunk(0, size)
		tsNs := time.Now().UnixNano()
		chunk.WriteDataAt(buf, 0, tsNs)
		chunks = append(chunks, chunk)
		return chunks
	} else {
		for i := 0; i < numChunks; i++ {
			buf := make([]byte, chunkSize)
			_, err := file.Read(buf)
			if err != nil {
				return nil
			}
			chunk := page_writer.NewMemChunk(page_writer.LogicChunkIndex(i), chunkSize)
			tsNs := time.Now().UnixNano()
			chunk.WriteDataAt(buf, 0, tsNs)
			chunks = append(chunks, chunk)
		}
	}

	return chunks
}
