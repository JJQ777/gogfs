package cache

import (
	"log"
	"os"
	"path/filepath"
	"sync"
)

// BlockCache: 简单的块缓存（内存 + 磁盘），按 blockID 作为 key
type BlockCache struct {
	dir string
	mu  sync.RWMutex
	mem map[string][]byte // 内存缓存
}

// NewBlockCache 创建/初始化缓存目录
func NewBlockCache(dir string) *BlockCache {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Printf("⚠️  Failed to create cache dir %s: %v", dir, err)
	}
	return &BlockCache{
		dir: dir,
		mem: make(map[string][]byte),
	}
}

// cacheFilePath: 根据 blockID 返回磁盘缓存文件路径
func (c *BlockCache) cacheFilePath(blockID string) string {
	// blockID 是 UUID，本身就比较安全，直接用
	filename := blockID + ".blk"
	return filepath.Join(c.dir, filename)
}

// Get: 尝试从缓存中读取 block
// 1) 先查内存 map
// 2) 再查磁盘文件（存在则读入内存）
func (c *BlockCache) Get(blockID string) ([]byte, bool) {
	c.mu.RLock()
	if data, ok := c.mem[blockID]; ok {
		c.mu.RUnlock()
		return data, true
	}
	c.mu.RUnlock()

	// 内存没有，尝试从磁盘读取
	path := c.cacheFilePath(blockID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	// 读到之后写回内存
	c.mu.Lock()
	c.mem[blockID] = data
	c.mu.Unlock()

	return data, true
}

// Put: 写入缓存（内存 + 磁盘）
func (c *BlockCache) Put(blockID string, data []byte) {
	// 写入内存
	c.mu.Lock()
	c.mem[blockID] = data
	c.mu.Unlock()

	// 写入磁盘
	path := c.cacheFilePath(blockID)
	if err := os.WriteFile(path, data, 0644); err != nil {
		log.Printf("⚠️  Failed to write cache file %s: %v", path, err)
	}
}
