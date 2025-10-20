package datanode

import (
	"context"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	datanodeService "github.com/JJQ777/gogfs/proto/datanode"
	namenodeService "github.com/JJQ777/gogfs/proto/namenode"
	"github.com/JJQ777/gogfs/utils"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DataNode struct {
	ID               string
	DataNodeLocation string
	Blocks           []string
	datanodeService.UnimplementedDatanodeServiceServer
}

func (datanode *DataNode) InitializeDataNode(port string, location string) {
	idFilePath := filepath.Join(location, "node_id.txt")

	if idData, err := os.ReadFile(idFilePath); err == nil {
		datanode.ID = string(idData)
		log.Printf("Loaded existing DataNode ID: %s\n", datanode.ID)
	} else {
		datanode.ID = uuid.New().String()
		err := os.WriteFile(idFilePath, []byte(datanode.ID), 0644)
		if err != nil {
			log.Fatalf("Failed to write node_id.txt: %v", err)
		}
		log.Printf("Generated new DataNode ID: %s", datanode.ID)
	}

	datanode.DataNodeLocation = filepath.Join(location, datanode.ID)
	CreateDirectory(datanode.DataNodeLocation)

	datanode.loadLocalBlocks()
	log.Printf("✅ DataNode %s initialized, found %d blocks\n", datanode.ID, len(datanode.Blocks))
}

func (datanode *DataNode) ConnectToNameNode(port string, host string) *grpc.ClientConn {
	connectionString := net.JoinHostPort(host, port)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return conn
}

func CreateDirectory(path string) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		pathCreationError := os.MkdirAll(path, os.ModePerm)
		utils.ErrorHandler(pathCreationError)
	} else {
		utils.ErrorHandler(err)
	}
}

func (datanode *DataNode) RegisterNode(conn *grpc.ClientConn, port string) {
	client := namenodeService.NewNamenodeServiceClient(conn)
	status, err := client.Register_DataNode(context.Background(), &namenodeService.DatanodeData{DatanodeID: datanode.ID, DatanodePort: port})
	utils.ErrorHandler(err)
	log.Printf("🧩 DataNode %s registration status: %s\n", datanode.ID, status.StatusMessage)
}

// persistance
func (datanode *DataNode) loadLocalBlocks() {
	files, err := os.ReadDir(datanode.DataNodeLocation)
	if err != nil {
		log.Printf("⚠️  Failed to scan local block directory: %v", err)
		return
	}
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".txt") {
			blockID := strings.TrimSuffix(f.Name(), ".txt")
			datanode.Blocks = append(datanode.Blocks, blockID)
		}
	}
}

func (datanode *DataNode) persistBlockList() {
	indexPath := filepath.Join(datanode.DataNodeLocation, "blocks_index.txt")
	f, err := os.Create(indexPath)
	if err != nil {
		log.Printf("❌ Failed to create blocks_index.txt: %v", err)
		return
	}
	defer f.Close()
	for _, b := range datanode.Blocks {
		f.WriteString(b + "\n")
	}
}

// write
func (datanode *DataNode) SendDataToDataNodes(ctx context.Context, clientToDataNodeRequest *datanodeService.ClientToDataNodeRequest) (*datanodeService.Status, error) {
	CreateDirectory(datanode.DataNodeLocation)
	blockFilePath := filepath.Join(datanode.DataNodeLocation, clientToDataNodeRequest.BlockID+".txt")

	err := os.WriteFile(blockFilePath, clientToDataNodeRequest.Content, os.ModePerm)
	if err != nil {
		log.Printf("❌ Failed to write block %s: %v", clientToDataNodeRequest.BlockID, err)
		return &datanodeService.Status{Message: "Failed"}, err
	}

	datanode.Blocks = append(datanode.Blocks, clientToDataNodeRequest.BlockID)
	datanode.persistBlockList()

	log.Printf("Block %s saved successfully on DataNode %s", clientToDataNodeRequest.BlockID, datanode.ID)
	utils.ErrorHandler(err)
	return &datanodeService.Status{Message: "Data saved successfully"}, nil
}

// read
func (datanode *DataNode) ReadBytesFromDataNode(ctx context.Context, blockRequest *datanodeService.BlockRequest) (*datanodeService.ByteResponse, error) {
	blockID := blockRequest.BlockID
	filePath := filepath.Join(datanode.DataNodeLocation, blockID+".txt")
	log.Println(filePath)
	content, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("❌ Failed to read block %s: %v", blockID, err)
		return nil, err
	}
	utils.ErrorHandler(err)
	return &datanodeService.ByteResponse{FileContent: content}, nil
}

// report
func (datanode *DataNode) SendBlockReport(conn *grpc.ClientConn) {

	nameNodeClient := namenodeService.NewNamenodeServiceClient(conn)
	datanodeBlockData := &namenodeService.DatanodeBlockData{DatanodeID: datanode.ID, Blocks: datanode.Blocks}
	status, err := nameNodeClient.BlockReport(context.Background(), datanodeBlockData)
	utils.ErrorHandler(err)
	log.Println(status.StatusMessage)
}

func (datanode *DataNode) SendBlockReportToNameNode(conn *grpc.ClientConn) {
	interval := 10 * time.Second
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			datanode.SendBlockReport(conn)
		}
	}

}

func (datanode *DataNode) StartServer(port string) {
	server := grpc.NewServer()
	datanodeService.RegisterDatanodeServiceServer(server, datanode)
	address := ":" + port
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("Datanode with id = %s is listening on port %s\n", datanode.ID, address)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
