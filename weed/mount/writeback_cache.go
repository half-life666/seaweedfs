package mount

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/page_writer"
)

const (
	writeBufferSize  = 512 * 1024
	defaultFileCount = 4000
)

type WritebackCache struct {
	sync.Mutex
	lruList        *lru.Cache[string, *TempFileHandle]
	chunkSize      int64
	uploadPipeline *page_writer.UploadPipeline
	dirtyPages     *ChunkedDirtyPages
	quotaSize      int64
	cacheDir       string
}

type TempFileHandle struct {
	path string
	file *os.File
	size int64
}

func NewWritebackCache(chunkSize int64, wfs *WFS, collection string, quotaLimitMB int64) *WritebackCache {
	cache, _ := lru.New[string, *TempFileHandle](defaultFileCount)

	dirtyPages := &ChunkedDirtyPages{
		collection: collection,
	}
	uploadPipeline := page_writer.NewUploadPipeline(wfs.concurrentWriters, chunkSize,
		dirtyPages.saveChunkedFileIntervalToStorage, wfs.option.ConcurrentWriters, wfs.option.CacheDirForWrite)

	return &WritebackCache{
		lruList:        cache,
		chunkSize:      chunkSize,
		uploadPipeline: uploadPipeline,
		dirtyPages:     dirtyPages,
		quotaSize:      quotaLimitMB << 20,
		cacheDir:       wfs.option.CacheDirForWrite,
	}
}

func (c *WritebackCache) OpenFile(filePath string) error {
	c.Lock()
	defer c.Unlock()

	tempFilePath := c.cacheDir + "/" + c.dirtyPages.collection + "/" + filePath

	if _, ok := c.lruList.Peek(tempFilePath); ok {
		return nil
	}

	f, err := os.OpenFile(tempFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	fileInfo, err := f.Stat()
	if err != nil {
		return err
	}

	glog.V(4).Infof("add temp file %s for %s, size: %d", filePath, tempFilePath, fileInfo.Size())
	c.lruList.Add(tempFilePath, &TempFileHandle{
		path: tempFilePath,
		file: f,
		size: fileInfo.Size(),
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
	var written int64
	for {
		buf := make([]byte, writeBufferSize)
		n, err := reader.Read(buf)
		if n > 0 {
			fh.file.Seek(offset, 0)
			fh.file.Write(buf[:n])
			written += int64(n)
			offset += int64(n)
			if written >= size {
				break
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return written, err
		}
	}
	fileInfo, err := fh.file.Stat()
	if err != nil {
		return 0, err
	}
	curSize := int64(fileInfo.Size())
	if fh.size != curSize {
		fh.size = curSize
		c.lruList.Add(path, fh)
	}

	return written, nil
}

func (c *WritebackCache) LoadTempFiles() error {
	files, err := os.ReadDir(c.cacheDir + "/" + c.dirtyPages.collection)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filePath := c.cacheDir + "/" + c.dirtyPages.collection + "/" + file.Name()
		f, err := os.OpenFile(filePath, os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		fileInfo, err := f.Stat()
		if err != nil {
			return err
		}
		glog.V(4).Infof("add temp file %s, size: %d", filePath, fileInfo.Size())
		c.lruList.Add(filePath, &TempFileHandle{
			path: filePath,
			file: f,
			size: fileInfo.Size(),
		})
	}
	return nil
}

func (c *WritebackCache) Run(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			filePath, fh, ok := c.lruList.GetOldest()
			if !ok {
				continue
			}
			if !c.checkCacheQuota() {
				continue
			}
			chunks := tempFileToMemChunks(fh.file, c.chunkSize)
			if len(chunks) == 0 {
				continue
			}
			glog.V(4).Infoln("uploading temp file, chunks", filePath, len(chunks))

			offset := int64(0)
			for _, chunk := range chunks {
				c.uploadPipeline.AddChunk(chunk, offset)
				offset += chunk.WrittenSize()
			}
			c.uploadPipeline.FlushAll()

			c.lruList.Remove(filePath)
			fh.file.Close()
			os.Remove(filePath)
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
