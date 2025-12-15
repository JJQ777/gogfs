package datanode

import (
	"context"
	"fmt"
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
	Port             string
	ConfigPath       string
	ActiveConn       *grpc.ClientConn
	StopChan         chan struct{} // Channel to signal goroutines to stop
	datanodeService.UnimplementedDatanodeServiceServer
}

func (datanode *DataNode) InitializeDataNode(port string, baseLocation string) {
	nodePath := filepath.Join(baseLocation, fmt.Sprintf("port_%s", port))
	CreateDirectory(nodePath)

	idFilePath := filepath.Join(nodePath, "node_id.txt")

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

	datanode.DataNodeLocation = filepath.Join(nodePath, datanode.ID)
	CreateDirectory(datanode.DataNodeLocation)
	
	// Initialize stop channel
	datanode.StopChan = make(chan struct{})

	datanode.loadLocalBlocks()
	log.Printf("✅ DataNode %s initialized at %s, found %d blocks\n",
		datanode.ID, datanode.DataNodeLocation, len(datanode.Blocks))
}

func (datanode *DataNode) ConnectToNameNode(port string, host string) *grpc.ClientConn {
	connectionString := net.JoinHostPort(host, port)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return conn
}

// ConnectToActiveNameNode uses auto-discovery to find and connect to active NameNode
func (datanode *DataNode) ConnectToActiveNameNode(configPath string) (*grpc.ClientConn, string, error) {
	config, err := utils.LoadClusterConfig(configPath)
	if err != nil {
		log.Printf("⚠️  Failed to load cluster config: %v, using default", err)
		config = &utils.NamenodeClusterConfig{
			Namenodes: []string{"localhost:8080", "localhost:8081"},
			Primary:   "localhost:8080",
		}
	}
	
	conn, activeAddr, err := utils.DiscoverActiveNamenode(config)
	if err != nil {
		return nil, "", err
	}
	
	return conn, activeAddr, nil
}

// MonitorConnectionAndReconnect monitors the connection and reconnects if necessary
func (datanode *DataNode) MonitorConnectionAndReconnect() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		// Try to ping NameNode
		if datanode.ActiveConn != nil {
			client := namenodeService.NewNamenodeServiceClient(datanode.ActiveConn)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := client.GetAvailableDatanodes(ctx, nil)
			cancel()
			
			if err != nil {
				log.Printf("⚠️  Connection to NameNode lost: %v", err)
				log.Printf("🔄 Attempting to reconnect...")
				
				// Stop the old block report goroutine
				close(datanode.StopChan)
				time.Sleep(1 * time.Second) // Give it time to stop
				
				// Close old connection
				datanode.ActiveConn.Close()
				
				// Try to reconnect
				conn, activeAddr, err := datanode.ConnectToActiveNameNode(datanode.ConfigPath)
				if err != nil {
					log.Printf("❌ Failed to reconnect: %v, will retry...", err)
					// Reinitialize stop channel for next attempt
					datanode.StopChan = make(chan struct{})
					continue
				}
				
				datanode.ActiveConn = conn
				log.Printf("✅ Reconnected to NameNode at %s", activeAddr)
				
				// Reinitialize stop channel
				datanode.StopChan = make(chan struct{})
				
				// Re-register with new NameNode
				datanode.RegisterNode(conn, datanode.Port)
				
				// Restart block report
				go datanode.SendBlockReportToNameNode(conn)
				log.Printf("🔄 Block report restarted")
			}
		}
	}
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
func (datanode *DataNode) SendBlockReport(conn *grpc.ClientConn) error {
	nameNodeClient := namenodeService.NewNamenodeServiceClient(conn)
	datanodeBlockData := &namenodeService.DatanodeBlockData{DatanodeID: datanode.ID, Blocks: datanode.Blocks}
	status, err := nameNodeClient.BlockReport(context.Background(), datanodeBlockData)
	if err != nil {
		return err
	}
	log.Println(status.StatusMessage)
	return nil
}

func (datanode *DataNode) SendBlockReportToNameNode(conn *grpc.ClientConn) {
	interval := 10 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			err := datanode.SendBlockReport(conn)
			if err != nil {
				log.Printf("⚠️  Failed to send block report: %v", err)
				// Don't panic, just log the error
				return
			}
		case <-datanode.StopChan:
			log.Printf("🛑 Block report stopped")
			return
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
