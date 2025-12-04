package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/JJQ777/gogfs/cache"
	"github.com/JJQ777/gogfs/checksum"
	datanodeService "github.com/JJQ777/gogfs/proto/datanode"
	namenodeService "github.com/JJQ777/gogfs/proto/namenode"
	"github.com/JJQ777/gogfs/utils"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ClientData struct {
	NameNodePort string
	Port         string
	Cache        *cache.BlockCache
}

type Block struct {
	blockID string
}

type Pair[T any, V any] struct {
	first  T
	second V
}

func (client *ClientData) InitializeClient(nameNodePort string) {
	client.NameNodePort = nameNodePort
	client.Cache = cache.NewBlockCache("./client-cache")
}
func (client *ClientData) ConnectToNameNode() *grpc.ClientConn {

	connectionString := net.JoinHostPort("localhost", client.NameNodePort)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return conn

}

// FIXED: Now takes both host and port
func GetDataNodeStub(host string, port string) datanodeService.DatanodeServiceClient {
	connectionString := net.JoinHostPort(host, port)
	conn, err := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("❌ Failed to connect to DataNode on %s:%s: %v", host, port, err)
	}
	dataNodeClient := datanodeService.NewDatanodeServiceClient(conn)
	return dataNodeClient
}

func (client *ClientData) GetNameNodeStub() namenodeService.NamenodeServiceClient {
	connectionString := net.JoinHostPort("localhost", client.NameNodePort)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	nameNodeClient := namenodeService.NewNamenodeServiceClient(conn)
	return nameNodeClient
}

func (client *ClientData) GetAvailableDatanodes(conn *grpc.ClientConn) (*namenodeService.FreeDataNodes, error) {

	namenodeClient := namenodeService.NewNamenodeServiceClient(conn)
	freeDataNodes, err := namenodeClient.GetAvailableDatanodes(context.Background(), &emptypb.Empty{})
	return freeDataNodes, err

}

func ThreadDone(done chan Pair[int, string], blockID string, idx int) {

	done <- Pair[int, string]{first: idx, second: blockID}
}

func SendData(datanode *namenodeService.DatanodeData, done chan Pair[int, string], blockID string, buffer []byte, n int, idx int) {
	// compute checksum
	checksumValue := checksum.ComputeChecksum(buffer[:n])

	clientDataNodeRequest := &datanodeService.ClientToDataNodeRequest{
		BlockID:  blockID,
		Content:  buffer[:n],
		Checksum: checksumValue,
	}
	datanodeClient := GetDataNodeStub(datanode.DatanodeHost, datanode.DatanodePort)
	response, err := datanodeClient.SendDataToDataNodes(context.Background(), clientDataNodeRequest)
	if err != nil {
		log.Printf("❌ Failed to send block %s to DataNode %s:%s: %v", blockID, datanode.DatanodeHost, datanode.DatanodePort, err)
		return
	}
	log.Printf("✅ Block %s sent to DataNode %s:%s (checksum: %s...): %s",
		blockID, datanode.DatanodeHost, datanode.DatanodePort, checksumValue[:8], response.Message)
	ThreadDone(done, blockID, idx)
}

func (client *ClientData) ProcessData(conn *grpc.ClientConn, blockSize int, done chan Pair[int, string], filePath string, start int, idx int) {

	blockID := uuid.New().String()

	fileHandler, err := os.Open(filePath)

	utils.ErrorHandler(err)
	fileInfo, err := fileHandler.Stat()
	fileSize := fileInfo.Size()
	end := start + blockSize
	if end > int(fileSize) {
		end = int(fileSize)
	}
	readBytes := end - start
	buffer := make([]byte, readBytes)
	n, err := fileHandler.ReadAt(buffer, int64(start))
	if err == io.EOF {
		return
	}
	utils.ErrorHandler(err)
	freeDataNodes, err := client.GetAvailableDatanodes(conn)
	utils.ErrorHandler(err)
	wg2 := &sync.WaitGroup{}
	for _, datanode := range freeDataNodes.DataNodeIDs {
		wg2.Add(1)
		go func(datanode *namenodeService.DatanodeData) {
			defer wg2.Done()
			SendData(datanode, done, blockID, buffer, n, idx)
		}(datanode)
	}
	wg2.Wait()
}

func (client *ClientData) SendFileBlockMappingToNameNode(filePath string, blockIDs []string) {

	nameNodeStub := client.GetNameNodeStub()
	fileBlockMetadata := &namenodeService.FileBlockMetadata{FilePath: filePath, BlockIDs: blockIDs}
	status, err := nameNodeStub.FileBlockMapping(context.Background(), fileBlockMetadata)
	utils.ErrorHandler(err)
	log.Println("Sent file block mapping to namenode with status:", status.StatusMessage)
}

func (client *ClientData) WriteFile(conn *grpc.ClientConn, sourcePath string, fileName string) {

	filePath := filepath.Join(sourcePath, fileName)

	blockSize := int(3 * 1024)
	fileInfo, err := os.Stat(filePath)
	utils.ErrorHandler(err)

	fileSize := int(fileInfo.Size())

	// 1️⃣ 正确计算 block 的 offset 列表
	var startList []int
	for offset := 0; offset < fileSize; offset += blockSize {
		startList = append(startList, offset)
	}
	numberOfBlocks := len(startList)

	log.Printf("📦 File size=%d bytes, blockSize=%d -> %d blocks",
		fileSize, blockSize, numberOfBlocks)

	// 2️⃣ 并发处理每个 block
	done := make(chan Pair[int, string])
	wg := &sync.WaitGroup{}

	for i, start := range startList {
		wg.Add(1)
		go func(idx int, start int) {
			defer wg.Done()
			client.ProcessData(conn, blockSize, done, filePath, start, idx)
		}(i, start)
	}

	// 3️⃣ 等待完成并收集 blockIDs
	go func() {
		wg.Wait()
		close(done)
	}()

	// 用来按顺序存储 blockIDs
	blockPairs := make([]Pair[int, string], 0, numberOfBlocks)
	for pair := range done {
		blockPairs = append(blockPairs, pair)
	}

	// 4️⃣ 按 block index 排序
	sort.Slice(blockPairs, func(i, j int) bool {
		return blockPairs[i].first < blockPairs[j].first
	})

	// 5️⃣ 提取 blockIDs
	var blockIDs []string
	for _, p := range blockPairs {
		blockIDs = append(blockIDs, p.second)
	}

	log.Println("📝 Final blockIDs:", blockIDs)

	// 6️⃣ 写入 FileToBlockMapping
	client.SendFileBlockMappingToNameNode(filePath, blockIDs)
}

func (client *ClientData) ReadFile(conn *grpc.ClientConn, source string, fileName string) {

	filePath := filepath.Join(source, fileName)
	nameNodeStub := client.GetNameNodeStub()

	// 1. 从 NameNode 获取所有 block → Datanode 映射
	fileData := &namenodeService.FileData{FileName: filePath}
	dataNodes, err := nameNodeStub.GetDataNodesForFile(context.Background(), fileData)
	utils.ErrorHandler(err)

	dataNodesBlocks := dataNodes.BlockDataNodes

	// 2. 为最终输出文件创建目录
	outputDir := "./client-output"
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		log.Fatalf("❌ Failed to create output directory: %v", err)
	}

	// 3. 创建本地输出文件（覆盖旧的）
	outputPath := filepath.Join(outputDir, fileName)
	out, err := os.Create(outputPath)
	if err != nil {
		log.Fatalf("❌ Failed to create output file %s: %v", outputPath, err)
	}
	defer out.Close()

	log.Printf("📥 Reading file from GFS → local file: %s", outputPath)

	// 4. 遍历所有 block，按顺序恢复文件
	for _, blockDataNode := range dataNodesBlocks {
		blockID := blockDataNode.BlockID
		dataNodeIDs := blockDataNode.DataNodeIDs

		var blockContent []byte
		var checksumValid bool

		// 4.1 尝试从缓存读取
		if client.Cache != nil {
			if cached, ok := client.Cache.Get(blockID); ok {
				log.Printf("🟢 Cache hit for block %s", blockID)
				blockContent = cached
				checksumValid = true
			} else {
				log.Printf("🔵 Cache miss for block %s", blockID)
			}
		}

		// 4.2 缓存未命中 → 从 DataNode 读取
		if !checksumValid {
			for _, dataNode := range dataNodeIDs {

				dnClient := GetDataNodeStub(dataNode.DatanodeHost, dataNode.DatanodePort)
				blockResponse, err := dnClient.ReadBytesFromDataNode(
					context.Background(),
					&datanodeService.BlockRequest{BlockID: blockID},
				)

				if err != nil {
					log.Printf("⚠️  Failed to read block %s from %s:%s: %v",
						blockID, dataNode.DatanodeHost, dataNode.DatanodePort, err)
					continue
				}

				if checksum.VerifyChecksum(blockResponse.FileContent, blockResponse.Checksum) {
					blockContent = blockResponse.FileContent
					checksumValid = true

					// 写入缓存
					if client.Cache != nil {
						client.Cache.Put(blockID, blockContent)
					}
					break
				} else {
					log.Printf("❌ CORRUPT block %s on %s:%s", blockID, dataNode.DatanodeHost, dataNode.DatanodePort)
				}
			}
		}

		if !checksumValid {
			utils.ErrorHandler(fmt.Errorf("all replicas corrupt for block %s", blockID))
		}

		// 4.3 将 block 写入输出文件
		_, err := out.Write(blockContent)
		if err != nil {
			log.Fatalf("❌ Failed writing block %s to output file: %v", blockID, err)
		}
	}

	log.Printf("🎉 File restored successfully: %s", outputPath)
}

func (client *ClientData) DeleteFile(conn *grpc.ClientConn, fileName string) {
	nameNodeStub := client.GetNameNodeStub()
	fileData := &namenodeService.FileData{FileName: fileName}
	status, err := nameNodeStub.DeleteFile(context.Background(), fileData)
	if err != nil {
		log.Printf("❌ Failed to delete file: %v", err)
		return
	}
	log.Printf("✅ %s", status.StatusMessage)
}

// reportCorruptBlock reports a corrupt block to NameNode
func (client *ClientData) reportCorruptBlock(blockID string, datanodeID string, reason string) {
	nameNodeStub := client.GetNameNodeStub()
	corruptReport := &namenodeService.CorruptBlockReport{
		BlockID:    blockID,
		DatanodeID: datanodeID,
		Reason:     reason,
	}

	status, err := nameNodeStub.ReportCorruptBlock(context.Background(), corruptReport)
	if err != nil {
		log.Printf("⚠️  Failed to report corrupt block: %v", err)
		return
	}
	log.Printf("📢 Reported corrupt block %s on node %s: %s", blockID, datanodeID, status.StatusMessage)
}
