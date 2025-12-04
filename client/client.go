package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

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
	fileSizeHandler, err := os.Stat(filePath)

	utils.ErrorHandler(err)

	fileSize := int(fileSizeHandler.Size())

	numberOfBlocks := fileSize / blockSize

	if fileSize%blockSize > 0 {
		numberOfBlocks++
	}

	done := make(chan Pair[int, string])
	startList := make([]int64, 0)

	amount := 0

	for {
		if amount > fileSize {
			break
		}
		startList = append(startList, int64(amount))
		amount += blockSize

	}
	wg1 := &sync.WaitGroup{}
	for i := 0; i < numberOfBlocks; i++ {
		wg1.Add(1)
		go func(start int, idx int) {
			defer wg1.Done()
			client.ProcessData(conn, blockSize, done, filePath, start, idx)
		}(int(startList[i]), i)
	}
	go func() {
		wg1.Wait()
		close(done)
	}()

	sortedblockIDs := make([]Pair[int, string], 0)
	for i := 0; i < numberOfBlocks; i++ {
		sortedblockIDs = append(sortedblockIDs, <-done)
	}
	for _, block := range sortedblockIDs {
		fmt.Printf("Before = %d\n", block.first)
	}
	sort.Slice(sortedblockIDs, func(i, j int) bool {
		return sortedblockIDs[i].first < sortedblockIDs[j].first
	})
	blockIDs := make([]string, 0)
	for _, block := range sortedblockIDs {
		fmt.Printf("After = %d\n", block.first)
		blockIDs = append(blockIDs, block.second)
	}
	client.SendFileBlockMappingToNameNode(filePath, blockIDs)

}

func (client *ClientData) ReadFile(conn *grpc.ClientConn, source string, fileName string) {

	filePath := filepath.Join(source, fileName)
	nameNodeStub := client.GetNameNodeStub()
	fileData := &namenodeService.FileData{FileName: filePath}
	dataNodes, err := nameNodeStub.GetDataNodesForFile(context.Background(), fileData)
	utils.ErrorHandler(err)
	rand.Seed(time.Now().UnixNano())
	dataNodesBlocks := dataNodes.BlockDataNodes

	for _, blockDataNode := range dataNodesBlocks {
		blockID := blockDataNode.BlockID
		dataNodeIDs := blockDataNode.DataNodeIDs

		var blockContent []byte
		var checksumValid bool

		// ✅ 1️⃣ 先尝试从缓存读取
		if client.Cache != nil {
			if cached, ok := client.Cache.Get(blockID); ok {
				log.Printf("🟢 Cache hit for block %s", blockID)
				blockContent = cached
				checksumValid = true
			} else {
				log.Printf("🔵 Cache miss for block %s, will fetch from DataNode", blockID)
			}
		}

		// ✅ 2️⃣ 如果缓存没有命中，才去 DataNode 读
		if !checksumValid {
			for attempt := 0; attempt < len(dataNodeIDs); attempt++ {
				dataNodeIdx := (rand.Intn(len(dataNodeIDs)) + attempt) % len(dataNodeIDs)
				dataNode := dataNodeIDs[dataNodeIdx]

				dataNodeClient := GetDataNodeStub(dataNode.DatanodeHost, dataNode.DatanodePort)
				blockRequest := &datanodeService.BlockRequest{BlockID: blockID}
				blockResponse, err := dataNodeClient.ReadBytesFromDataNode(context.Background(), blockRequest)

				if err != nil {
					log.Printf("⚠️  Failed to read block %s from %s:%s: %v",
						blockID, dataNode.DatanodeHost, dataNode.DatanodePort, err)

					// if checksum is wrong report to namenode
					if strings.Contains(err.Error(), "corruption") || strings.Contains(err.Error(), "checksum") {
						client.reportCorruptBlock(blockID, dataNode.DatanodeID, "Checksum verification failed on DataNode")
					}
					continue
				}

				if checksum.VerifyChecksum(blockResponse.FileContent, blockResponse.Checksum) {
					log.Printf("✅ Block %s checksum verified from %s:%s",
						blockID, dataNode.DatanodeHost, dataNode.DatanodePort)
					blockContent = blockResponse.FileContent
					checksumValid = true

					// ✅ 3️⃣ 把从 DataNode 拉到的块写入缓存
					if client.Cache != nil {
						client.Cache.Put(blockID, blockContent)
					}
					break
				} else {
					log.Printf("❌ CORRUPT BLOCK DETECTED! Block %s on %s:%s failed checksum",
						blockID, dataNode.DatanodeHost, dataNode.DatanodePort)

					client.reportCorruptBlock(blockID, dataNode.DatanodeID, "Checksum mismatch")
					continue
				}
			}
		}

		if !checksumValid {
			log.Printf("💥 CRITICAL: All replicas of block %s are corrupt!", blockID)
			utils.ErrorHandler(fmt.Errorf("all replicas corrupt for block %s", blockID))
		}

		// ⚠️ 这里你现在是直接 Println string(blockContent)，
		// 实际系统应该是按顺序写回一个文件，这里先保持你原逻辑不动。
		log.Println(string(blockContent))
	}
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
