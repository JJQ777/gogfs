package namenode

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	namenode "github.com/JJQ777/gogfs/proto/namenode"
	"github.com/JJQ777/gogfs/utils"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const MetadataFile = "metadata.json"

type DataNodeMetadata struct {
	ID     string `json:"id"`
	Port   string `json:"port"`
	Status string `json:"status"`
}
type PersistentMetadata struct {
	FileToBlockMapping        map[string][]string         `json:"file_to_block"`
	DataNodeToBlockMapping    map[string][]string         `json:"datanode_to_blocks"`
	DataNodeToMetadataMapping map[string]DataNodeMetadata `json:"datanode_metadata"`
}
type NameNodeData struct {
	BlockSize                 int64
	DataNodeToBlockMapping    map[string][]string
	ReplicationFactor         int64
	DataNodeToMetadataMapping map[string]DataNodeMetadata
	FileToBlockMapping        map[string][]string

	// HotBackup: Standby NameNode support
	ID                string
	Role              string // "active" or "standby"
	StandbyAddress    string
	ActiveAddress     string
	LastHeartbeat     int64
	StandbyConnection *grpc.ClientConn

	metaLock sync.RWMutex

	namenode.UnimplementedNamenodeServiceServer
}

type DataNodeBlockCount struct {
	DataNodeData *namenode.DatanodeData
	BlockCount   int64
}

func (nameNode *NameNodeData) InitializeNameNode(port string, blockSize int64, role string, peerAddress string) {

	nameNode.BlockSize = blockSize
	nameNode.ReplicationFactor = 3
	nameNode.ID = uuid.New().String()
	nameNode.Role = role

	// Set peer addresses
	if role == "active" {
		nameNode.StandbyAddress = peerAddress
	} else if role == "standby" {
		nameNode.ActiveAddress = peerAddress
	}

	nameNode.loadMetadataFromJSON(MetadataFile)

	if nameNode.DataNodeToBlockMapping == nil {
		nameNode.DataNodeToBlockMapping = make(map[string][]string)
	}
	if nameNode.DataNodeToMetadataMapping == nil {
		nameNode.DataNodeToMetadataMapping = make(map[string]DataNodeMetadata)
	}
	if nameNode.FileToBlockMapping == nil {
		nameNode.FileToBlockMapping = make(map[string][]string)
	}

	server := grpc.NewServer()
	namenode.RegisterNamenodeServiceServer(server, nameNode)
	address := ":" + port
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("🔷 NameNode [%s] is listening on port %s as %s", nameNode.ID[:8], address, strings.ToUpper(nameNode.Role))

	// Start heartbeat monitoring if standby
	if nameNode.Role == "standby" && nameNode.ActiveAddress != "" {
		log.Printf("🔄 Standby NameNode monitoring Active at %s", nameNode.ActiveAddress)
		go nameNode.monitorActiveNameNode()
	}

	// Start syncing to standby if active
	if nameNode.Role == "active" && nameNode.StandbyAddress != "" {
		log.Printf("🔄 Active NameNode will sync to Standby at %s", nameNode.StandbyAddress)
		go nameNode.setupStandbyConnection()
	}

	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// persistance
func (nameNode *NameNodeData) persistMetadataToJSON(filename string) {
	data := PersistentMetadata{
		FileToBlockMapping:        nameNode.FileToBlockMapping,
		DataNodeToBlockMapping:    nameNode.DataNodeToBlockMapping,
		DataNodeToMetadataMapping: nameNode.DataNodeToMetadataMapping,
	}

	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal metadata: %v", err)
		return
	}
	if err := os.WriteFile(filename, jsonBytes, 0644); err != nil {
		log.Printf("Failed to write %s: %v", MetadataFile, err)
	} else {
		log.Printf("Metadata persisted to %s", MetadataFile)
	}
}
func (nameNode *NameNodeData) loadMetadataFromJSON(filename string) {
	file, err := os.ReadFile(filename)
	if err != nil {
		log.Printf("No existing %s found, starting fresh", MetadataFile)
		return
	}
	var data PersistentMetadata
	if err := json.Unmarshal(file, &data); err != nil {
		log.Printf("Failed to unmarshal %s: %v", MetadataFile, err)
		return
	}

	nameNode.FileToBlockMapping = data.FileToBlockMapping
	nameNode.DataNodeToBlockMapping = data.DataNodeToBlockMapping
	nameNode.DataNodeToMetadataMapping = data.DataNodeToMetadataMapping

	log.Printf("✅ Metadata loaded from %s", filename)
}

// RPC Methods
func (nameNode *NameNodeData) Register_DataNode(
	ctx context.Context,
	datanodeData *namenode.DatanodeData,
) (*namenode.Status, error) {

	nameNode.metaLock.Lock()

	_, exists := nameNode.DataNodeToBlockMapping[datanodeData.DatanodeID]
	if !exists {
		nameNode.DataNodeToBlockMapping[datanodeData.DatanodeID] = make([]string, 0)
		dnMeta := DataNodeMetadata{
			ID: datanodeData.DatanodeID, Port: datanodeData.DatanodePort, Status: "Available",
		}
		nameNode.DataNodeToMetadataMapping[datanodeData.DatanodeID] = dnMeta
	}

	nameNode.metaLock.Unlock() // 🔓 提前释放锁再写 JSON
	nameNode.persistMetadataToJSON("metadata.json")

	log.Printf("✅ Registered DataNode %s on port %s", datanodeData.DatanodeID, datanodeData.DatanodePort)
	return &namenode.Status{StatusMessage: "Registered"}, nil
}

func (nameNode *NameNodeData) GetAvailableDatanodes(ctx context.Context, empty *empty.Empty) (freeNodes *namenode.FreeDataNodes, err error) {
	nameNode.metaLock.RLock()
	defer nameNode.metaLock.RUnlock()

	availableDataNodes := make([]*DataNodeBlockCount, 0)
	freeDataNodes := make([]*namenode.DatanodeData, 0)
	for dataNodeID, datanodeMetadata := range nameNode.DataNodeToMetadataMapping {
		if datanodeMetadata.Status == "Available" {
			datanodeData := &namenode.DatanodeData{DatanodeID: dataNodeID, DatanodePort: nameNode.DataNodeToMetadataMapping[dataNodeID].Port}
			blockCount := int64(len(nameNode.DataNodeToBlockMapping[dataNodeID]))
			dataNodeBlockCount := &DataNodeBlockCount{DataNodeData: datanodeData, BlockCount: blockCount}
			availableDataNodes = append(availableDataNodes, dataNodeBlockCount)
		}
	}

	sort.SliceStable(availableDataNodes, func(i, j int) bool {
		return availableDataNodes[i].BlockCount < availableDataNodes[j].BlockCount
	})
	replicaCount := int(nameNode.ReplicationFactor)
	if len(availableDataNodes) < replicaCount {
		replicaCount = len(availableDataNodes)
	}
	for i := 0; i < replicaCount; i++ {
		freeDataNode := &namenode.DatanodeData{DatanodeID: availableDataNodes[i].DataNodeData.DatanodeID, DatanodePort: availableDataNodes[i].DataNodeData.DatanodePort}
		freeDataNodes = append(freeDataNodes, freeDataNode)
	}
	return &namenode.FreeDataNodes{DataNodeIDs: freeDataNodes[:replicaCount]}, nil

}

func (nameNode *NameNodeData) BlockReport(ctx context.Context, dataNodeBlockData *namenode.DatanodeBlockData) (status *namenode.Status, err error) {
	nameNode.metaLock.Lock()

	nameNode.DataNodeToBlockMapping[dataNodeBlockData.DatanodeID] = dataNodeBlockData.Blocks
	nameNode.metaLock.Unlock()
	nameNode.persistMetadataToJSON(MetadataFile)
	
	// Sync to standby if active
	if nameNode.Role == "active" {
		go nameNode.syncToStandby()
	}
	
	return &namenode.Status{StatusMessage: "Block Report Received"}, nil
}

func (nameNode *NameNodeData) FindDataNodesByBlock(blockID string) []DataNodeMetadata {
	nameNode.metaLock.RLock()
	defer nameNode.metaLock.RUnlock()

	dataNodes := make([]DataNodeMetadata, 0)
	for dataNode, blocks := range nameNode.DataNodeToBlockMapping {
		if utils.ValueInArray(blockID, blocks) {
			dataNodes = append(dataNodes, nameNode.DataNodeToMetadataMapping[dataNode])
		}

	}
	return dataNodes
}

func (nameNode *NameNodeData) GetDataNodesForFile(ctx context.Context, fileData *namenode.FileData) (*namenode.BlockData, error) {
	nameNode.metaLock.RLock()
	defer nameNode.metaLock.RUnlock()

	blocks, ok := nameNode.FileToBlockMapping[fileData.FileName]
	dataNodes := make([]*namenode.BlockDataNode, 0)
	if !ok {
		return nil, errors.New("file does not exist")
	}
	for _, block := range blocks {
		dataNodeList := nameNode.FindDataNodesByBlock(block)
		dataNodeIDsList := make([]*namenode.DatanodeData, 0)
		for _, datanode := range dataNodeList {
			dataNodeIDsList = append(dataNodeIDsList, &namenode.DatanodeData{DatanodeID: datanode.ID, DatanodePort: datanode.Port})
		}
		blockData := &namenode.BlockDataNode{BlockID: block, DataNodeIDs: dataNodeIDsList}
		dataNodes = append(dataNodes, blockData)
	}

	return &namenode.BlockData{BlockDataNodes: dataNodes}, nil

}

func (nameNode *NameNodeData) FileBlockMapping(ctx context.Context, fileBlockMetadata *namenode.FileBlockMetadata) (*namenode.Status, error) {
	nameNode.metaLock.Lock()

	filePath := fileBlockMetadata.FilePath
	blockIDs := fileBlockMetadata.BlockIDs

	nameNode.FileToBlockMapping[filePath] = blockIDs
	nameNode.metaLock.Unlock()
	nameNode.persistMetadataToJSON(MetadataFile)
	
	// Sync to standby if active
	if nameNode.Role == "active" {
		go nameNode.syncToStandby()
	}
	
	return &namenode.Status{StatusMessage: "Success"}, nil
}

// ============================================================================
// HotBackup: Standby NameNode Support Methods
// ============================================================================

func (nameNode *NameNodeData) setupStandbyConnection() {
	time.Sleep(2 * time.Second) // Wait for standby to start
	
	conn, err := grpc.Dial(nameNode.StandbyAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("⚠️  Failed to connect to standby NameNode: %v", err)
		return
	}
	nameNode.StandbyConnection = conn
	log.Printf("✅ Connected to Standby NameNode at %s", nameNode.StandbyAddress)
	
	// Perform initial sync
	nameNode.syncToStandby()
	
	// Start periodic sync
	go nameNode.periodicSync()
}

func (nameNode *NameNodeData) periodicSync() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		if nameNode.Role == "active" {
			nameNode.syncToStandby()
		}
	}
}

func (nameNode *NameNodeData) syncToStandby() {
	if nameNode.StandbyConnection == nil {
		return
	}
	
	nameNode.metaLock.RLock()
	
	// Convert metadata to proto format
	metadata := &namenode.Metadata{
		FileToBlock:      make(map[string]*namenode.BlockList),
		DatanodeToBlocks: make(map[string]*namenode.BlockList),
		DatanodeMetadata: make(map[string]*namenode.DatanodeMetadata),
	}
	
	for file, blocks := range nameNode.FileToBlockMapping {
		metadata.FileToBlock[file] = &namenode.BlockList{Blocks: blocks}
	}
	
	for dn, blocks := range nameNode.DataNodeToBlockMapping {
		metadata.DatanodeToBlocks[dn] = &namenode.BlockList{Blocks: blocks}
	}
	
	for dnID, dnMeta := range nameNode.DataNodeToMetadataMapping {
		metadata.DatanodeMetadata[dnID] = &namenode.DatanodeMetadata{
			Id:     dnMeta.ID,
			Port:   dnMeta.Port,
			Status: dnMeta.Status,
		}
	}
	
	nameNode.metaLock.RUnlock()
	
	client := namenode.NewNamenodeServiceClient(nameNode.StandbyConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	
	_, err := client.SyncMetadata(ctx, metadata)
	if err != nil {
		log.Printf("⚠️  Failed to sync to standby: %v", err)
	} else {
		log.Printf("📤 Synced metadata to Standby (Files: %d, DataNodes: %d)",
			len(nameNode.FileToBlockMapping), len(nameNode.DataNodeToMetadataMapping))
	}
}

func (nameNode *NameNodeData) monitorActiveNameNode() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	
	consecutiveFailures := 0
	maxFailures := 3
	
	for range ticker.C {
		conn, err := grpc.Dial(nameNode.ActiveAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(1*time.Second))
		
		if err != nil {
			consecutiveFailures++
			log.Printf("⚠️  Failed to connect to Active NameNode (%d/%d): %v",
				consecutiveFailures, maxFailures, err)
			
			if consecutiveFailures >= maxFailures {
				log.Printf("🚨 Active NameNode is DOWN! Promoting to Active...")
				nameNode.promoteToActive()
				return
			}
			continue
		}
		
		client := namenode.NewNamenodeServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		
		resp, err := client.Heartbeat(ctx, &namenode.NamenodeInfo{
			NamenodeID: nameNode.ID,
			Role:       "standby",
			Timestamp:  time.Now().Unix(),
		})
		
		cancel()
		conn.Close()
		
		if err != nil {
			consecutiveFailures++
			log.Printf("⚠️  Heartbeat failed (%d/%d): %v", consecutiveFailures, maxFailures, err)
			
			if consecutiveFailures >= maxFailures {
				log.Printf("🚨 Active NameNode is DOWN! Promoting to Active...")
				nameNode.promoteToActive()
				return
			}
		} else {
			if consecutiveFailures > 0 {
				log.Printf("✅ Active NameNode recovered")
			}
			consecutiveFailures = 0
			if resp.Role != "active" {
				log.Printf("⚠️  Active NameNode role mismatch: %s", resp.Role)
			}
		}
	}
}

func (nameNode *NameNodeData) promoteToActive() {
	nameNode.metaLock.Lock()
	nameNode.Role = "active"
	nameNode.metaLock.Unlock()
	
	log.Printf("=" + strings.Repeat("=", 70))
	log.Printf("✅ Successfully promoted to ACTIVE NameNode [%s]", nameNode.ID[:8])
	log.Printf("📋 Current metadata state:")
	log.Printf("   - Files: %d", len(nameNode.FileToBlockMapping))
	log.Printf("   - DataNodes: %d", len(nameNode.DataNodeToMetadataMapping))
	log.Printf("   - Total Blocks: %d", countTotalBlocks(nameNode.DataNodeToBlockMapping))
	log.Printf("=" + strings.Repeat("=", 70))
}

func countTotalBlocks(mapping map[string][]string) int {
	total := 0
	for _, blocks := range mapping {
		total += len(blocks)
	}
	return total
}

// ============================================================================
// RPC Methods for Standby Support
// ============================================================================

func (nameNode *NameNodeData) SyncMetadata(ctx context.Context, metadata *namenode.Metadata) (*namenode.Status, error) {
	if nameNode.Role != "standby" {
		return &namenode.Status{StatusMessage: "Not a standby node"}, nil
	}
	
	nameNode.metaLock.Lock()
	defer nameNode.metaLock.Unlock()
	
	// Update file to block mapping
	nameNode.FileToBlockMapping = make(map[string][]string)
	for file, blockList := range metadata.FileToBlock {
		nameNode.FileToBlockMapping[file] = blockList.Blocks
	}
	
	// Update datanode to blocks mapping
	nameNode.DataNodeToBlockMapping = make(map[string][]string)
	for dn, blockList := range metadata.DatanodeToBlocks {
		nameNode.DataNodeToBlockMapping[dn] = blockList.Blocks
	}
	
	// Update datanode metadata
	nameNode.DataNodeToMetadataMapping = make(map[string]DataNodeMetadata)
	for dnID, dnMeta := range metadata.DatanodeMetadata {
		nameNode.DataNodeToMetadataMapping[dnID] = DataNodeMetadata{
			ID:     dnMeta.Id,
			Port:   dnMeta.Port,
			Status: dnMeta.Status,
		}
	}
	
	// Persist to disk
	nameNode.persistMetadataToJSON(MetadataFile)
	
	log.Printf("📥 Synced metadata from Active (Files: %d, DataNodes: %d)",
		len(nameNode.FileToBlockMapping), len(nameNode.DataNodeToMetadataMapping))
	
	return &namenode.Status{StatusMessage: "Metadata synced successfully"}, nil
}

func (nameNode *NameNodeData) Heartbeat(ctx context.Context, info *namenode.NamenodeInfo) (*namenode.NamenodeInfo, error) {
	nameNode.LastHeartbeat = time.Now().Unix()
	
	return &namenode.NamenodeInfo{
		NamenodeID: nameNode.ID,
		Role:       nameNode.Role,
		Timestamp:  nameNode.LastHeartbeat,
	}, nil
}

func (nameNode *NameNodeData) PromoteToActive(ctx context.Context, empty *empty.Empty) (*namenode.Status, error) {
	if nameNode.Role == "active" {
		return &namenode.Status{StatusMessage: "Already active"}, nil
	}
	
	nameNode.promoteToActive()
	return &namenode.Status{StatusMessage: "Promoted to active"}, nil
}

