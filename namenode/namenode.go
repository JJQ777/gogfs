package namenode

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/JJQ777/gogfs/checksum"

	datanodeService "github.com/JJQ777/gogfs/proto/datanode"
	namenode "github.com/JJQ777/gogfs/proto/namenode"
	"github.com/JJQ777/gogfs/utils"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const MetadataFile = "metadata.json"

type DataNodeMetadata struct {
	ID            string    `json:"id"`
	Port          string    `json:"port"`
	Host          string    `json:"host"`
	Status        string    `json:"status"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
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

	HeartbeatTimeout time.Duration

	metaLock sync.RWMutex

	namenode.UnimplementedNamenodeServiceServer
}

type DataNodeBlockCount struct {
	DataNodeData *namenode.DatanodeData
	BlockCount   int64
}

func (nameNode *NameNodeData) InitializeNameNode(port string, blockSize int64) {

	nameNode.BlockSize = blockSize
	nameNode.ReplicationFactor = 3
	nameNode.HeartbeatTimeout = 30 * time.Second // --- NEW: Set heartbeat timeout (e.g., 30s)

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

	// --- NEW: Start the heartbeat monitor ---
	go nameNode.monitorDataNodes()
	// ---

	log.Printf("Namenode is listening on port %s\n", address)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// persistance
func (nameNode *NameNodeData) persistMetadataToJSON(filename string) {
	// --- MODIFIED: This function should be called WITH the lock held ---
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

	// --- NEW: Reset heartbeat timers on load ---
	for id, meta := range nameNode.DataNodeToMetadataMapping {
		meta.LastHeartbeat = time.Now()
		// We assume nodes are available on startup, monitor will correct this
		meta.Status = "Available"
		nameNode.DataNodeToMetadataMapping[id] = meta
	}
	// ---

	log.Printf("✅ Metadata loaded from %s", filename)
}

// RPC Methods
func (nameNode *NameNodeData) Register_DataNode(
	ctx context.Context,
	datanodeData *namenode.DatanodeData,
) (*namenode.Status, error) {

	nameNode.metaLock.Lock()
	defer nameNode.metaLock.Unlock() // --- MODIFIED: Use defer and lock for whole function ---

	_, exists := nameNode.DataNodeToBlockMapping[datanodeData.DatanodeID]
	if !exists {
		nameNode.DataNodeToBlockMapping[datanodeData.DatanodeID] = make([]string, 0)
	}

	// --- MODIFIED: Store new Host and LastHeartbeat info ---
	dnMeta := DataNodeMetadata{
		ID:            datanodeData.DatanodeID,
		Port:          datanodeData.DatanodePort,
		Host:          datanodeData.DatanodeHost, // <-- NEW
		Status:        "Available",
		LastHeartbeat: time.Now(), // <-- NEW
	}
	nameNode.DataNodeToMetadataMapping[datanodeData.DatanodeID] = dnMeta
	// ---

	// --- MODIFIED: Persist *inside* the lock ---
	nameNode.persistMetadataToJSON("metadata.json")

	log.Printf("✅ Registered DataNode %s on %s:%s", datanodeData.DatanodeID, datanodeData.DatanodeHost, datanodeData.DatanodePort)
	return &namenode.Status{StatusMessage: "Registered"}, nil
}

func (nameNode *NameNodeData) GetAvailableDatanodes(ctx context.Context, empty *empty.Empty) (freeNodes *namenode.FreeDataNodes, err error) {
	nameNode.metaLock.RLock()
	defer nameNode.metaLock.RUnlock()

	availableDataNodes := make([]*DataNodeBlockCount, 0)
	freeDataNodes := make([]*namenode.DatanodeData, 0)
	for dataNodeID, datanodeMetadata := range nameNode.DataNodeToMetadataMapping {
		if datanodeMetadata.Status == "Available" {
			// --- MODIFIED: Include Host in the datanodeData
			datanodeData := &namenode.DatanodeData{
				DatanodeID:   dataNodeID,
				DatanodePort: datanodeMetadata.Port,
				DatanodeHost: datanodeMetadata.Host, // <-- NEW
			}
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
		// --- MODIFIED: Include Host when creating the final list
		freeDataNode := &namenode.DatanodeData{
			DatanodeID:   availableDataNodes[i].DataNodeData.DatanodeID,
			DatanodePort: availableDataNodes[i].DataNodeData.DatanodePort,
			DatanodeHost: availableDataNodes[i].DataNodeData.DatanodeHost, // <-- NEW
		}
		freeDataNodes = append(freeDataNodes, freeDataNode)
	}
	return &namenode.FreeDataNodes{DataNodeIDs: freeDataNodes[:replicaCount]}, nil

}

// --- MODIFIED: BlockReport now also functions as a Heartbeat ---
func (nameNode *NameNodeData) BlockReport(ctx context.Context, dataNodeBlockData *namenode.DatanodeBlockData) (status *namenode.Status, err error) {
	nameNode.metaLock.Lock()
	defer nameNode.metaLock.Unlock()

	// --- NEW: Update heartbeat timestamp and status ---
	if dnMeta, ok := nameNode.DataNodeToMetadataMapping[dataNodeBlockData.DatanodeID]; ok {
		dnMeta.LastHeartbeat = time.Now()
		if dnMeta.Status == "Dead" {
			log.Printf("❤️ DataNode %s has re-registered (was marked Dead).", dnMeta.ID)
		}
		dnMeta.Status = "Available"
		nameNode.DataNodeToMetadataMapping[dataNodeBlockData.DatanodeID] = dnMeta

		// Now update the block mapping (original logic)
		nameNode.DataNodeToBlockMapping[dataNodeBlockData.DatanodeID] = dataNodeBlockData.Blocks

		// Persist inside the lock
		nameNode.persistMetadataToJSON(MetadataFile)
		return &namenode.Status{StatusMessage: "Block Report Received"}, nil

	} else {
		// Node is not registered, reject its block report
		log.Printf("Block report from unregistered datanode: %s. Ignoring.", dataNodeBlockData.DatanodeID)
		return &namenode.Status{StatusMessage: "Error: Node not registered"}, errors.New("datanode not registered")
	}
}

func (nameNode *NameNodeData) FindDataNodesByBlock(blockID string) []DataNodeMetadata {
	nameNode.metaLock.RLock()
	defer nameNode.metaLock.RUnlock()

	dataNodes := make([]DataNodeMetadata, 0)
	for dataNode, blocks := range nameNode.DataNodeToBlockMapping {
		if utils.ValueInArray(blockID, blocks) {
			// --- MODIFIED: Only return Available nodes ---
			if meta, ok := nameNode.DataNodeToMetadataMapping[dataNode]; ok && meta.Status == "Available" {
				dataNodes = append(dataNodes, meta)
			}
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
		dataNodeList := nameNode.FindDataNodesByBlock(block) // This now only returns *Available* nodes
		dataNodeIDsList := make([]*namenode.DatanodeData, 0)
		for _, datanode := range dataNodeList {
			// --- MODIFIED: Include Host ---
			dataNodeIDsList = append(dataNodeIDsList, &namenode.DatanodeData{
				DatanodeID:   datanode.ID,
				DatanodePort: datanode.Port,
				DatanodeHost: datanode.Host, // <-- NEW
			})
		}
		blockData := &namenode.BlockDataNode{BlockID: block, DataNodeIDs: dataNodeIDsList}
		dataNodes = append(dataNodes, blockData)
	}

	return &namenode.BlockData{BlockDataNodes: dataNodes}, nil

}

func (nameNode *NameNodeData) FileBlockMapping(ctx context.Context, fileBlockMetadata *namenode.FileBlockMetadata) (*namenode.Status, error) {
	nameNode.metaLock.Lock()
	defer nameNode.metaLock.Unlock() // --- MODIFIED: Use defer

	filePath := fileBlockMetadata.FilePath
	blockIDs := fileBlockMetadata.BlockIDs

	nameNode.FileToBlockMapping[filePath] = blockIDs

	nameNode.persistMetadataToJSON(MetadataFile) // --- MODIFIED: Persist inside lock
	return &namenode.Status{StatusMessage: "Success"}, nil

}

// ---
// ---
// --- NEW METHODS FOR HEARTBEAT AND RE-REPLICATION ---
// ---
// ---

// monitorDataNodes runs in a loop to check for dead datanodes
func (nameNode *NameNodeData) monitorDataNodes() {
	log.Println("❤️ Heartbeat monitor started")
	// Check every 10 seconds
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		nameNode.metaLock.Lock() // Full lock to modify status

		deadNodes := []string{}
		for id, meta := range nameNode.DataNodeToMetadataMapping {
			// Check if node is "Available" and has missed its heartbeat
			if meta.Status == "Available" && time.Since(meta.LastHeartbeat) > nameNode.HeartbeatTimeout {
				log.Printf("💔 DataNode %s timed out. Marking as 'Dead'.", id)
				meta.Status = "Dead"
				nameNode.DataNodeToMetadataMapping[id] = meta
				deadNodes = append(deadNodes, id)
			}
		}
		nameNode.metaLock.Unlock()

		// Trigger re-replication for all newly dead nodes
		// This is outside the lock to avoid long-running RPCs blocking metadata access
		for _, deadNodeID := range deadNodes {
			log.Printf("Triggering re-replication for dead node: %s", deadNodeID)
			go nameNode.handleDeadDataNode(deadNodeID)
		}
	}
}

// handleDeadDataNode finds all blocks on a dead node and triggers re-replication
func (nameNode *NameNodeData) handleDeadDataNode(deadNodeID string) {
	nameNode.metaLock.RLock()
	blocksToReplicate, ok := nameNode.DataNodeToBlockMapping[deadNodeID]
	if !ok {
		log.Printf("No block mapping found for dead node: %s", deadNodeID)
		nameNode.metaLock.RUnlock()
		return
	}
	// Create a copy of the block list to avoid holding the lock
	blocks := make([]string, len(blocksToReplicate))
	copy(blocks, blocksToReplicate)
	nameNode.metaLock.RUnlock()

	log.Printf("Node %s has %d blocks to re-replicate.", deadNodeID, len(blocks))
	for _, blockID := range blocks {
		nameNode.reReplicateBlock(blockID, deadNodeID)
	}
}

// reReplicateBlock finds a new home for a block that was on a dead node
func (nameNode *NameNodeData) reReplicateBlock(blockID string, deadNodeID string) {
	nameNode.metaLock.Lock() // Need full lock to find source/target and update map
	defer nameNode.metaLock.Unlock()

	// 1. Find a living source node that has the block
	var sourceNode DataNodeMetadata
	foundSource := false
	for dnID, blocks := range nameNode.DataNodeToBlockMapping {
		meta := nameNode.DataNodeToMetadataMapping[dnID]
		// Also exclude the dead node
		if meta.Status == "Available" &&
			dnID != deadNodeID && // <-- ADD THIS
			utils.ValueInArray(blockID, blocks) {
			sourceNode = meta
			foundSource = true
			break
		}
	}

	if !foundSource {
		log.Printf("CRITICAL: Block %s is lost! No living source node found.", blockID)
		return
	}

	// 2. Find a new target node
	// We re-use the GetAvailableDatanodes logic (sorted by block count)
	availableDataNodes := make([]*DataNodeBlockCount, 0)
	for dataNodeID, datanodeMetadata := range nameNode.DataNodeToMetadataMapping {
		if datanodeMetadata.Status == "Available" {
			datanodeData := &namenode.DatanodeData{
				DatanodeID:   dataNodeID,
				DatanodePort: datanodeMetadata.Port,
				DatanodeHost: datanodeMetadata.Host,
			}
			blockCount := int64(len(nameNode.DataNodeToBlockMapping[dataNodeID]))
			dataNodeBlockCount := &DataNodeBlockCount{DataNodeData: datanodeData, BlockCount: blockCount}
			availableDataNodes = append(availableDataNodes, dataNodeBlockCount)
		}
	}
	sort.SliceStable(availableDataNodes, func(i, j int) bool {
		return availableDataNodes[i].BlockCount < availableDataNodes[j].BlockCount
	})

	var targetNode DataNodeMetadata
	foundTarget := false
	for _, dnBlockCount := range availableDataNodes {
		targetID := dnBlockCount.DataNodeData.DatanodeID
		// Ensure target doesn't already have the block
		if !utils.ValueInArray(blockID, nameNode.DataNodeToBlockMapping[targetID]) {
			targetNode = nameNode.DataNodeToMetadataMapping[targetID]
			foundTarget = true
			break
		}
	}

	if !foundTarget {
		log.Printf("No suitable target node found for re-replicating block %s.", blockID)
		return
	}

	// 3. Orchestrate the copy
	// We must release the lock to make gRPC calls, but this is complex.
	// For simplicity, we'll make a copy of the data and release the lock.
	// A better design would use channels, but let's keep it simple.
	// *** MODIFICATION: We will hold the lock, but this is slow. ***
	// *** A better production design would release the lock ***
	log.Printf("Re-replicating block %s (from %s) -> %s", blockID, sourceNode.ID, targetNode.ID)

	// We must *temporarily* release the lock to make slow network calls
	nameNode.metaLock.Unlock()
	blockBytes, err := nameNode.readBlockFromDataNode(sourceNode, blockID)
	if err != nil {
		log.Printf("Failed to read block %s from source %s: %v", blockID, sourceNode.ID, err)
		nameNode.metaLock.Lock() // Re-acquire lock before returning
		return
	}

	err = nameNode.writeBlockToDataNode(targetNode, blockID, blockBytes)
	if err != nil {
		log.Printf("Failed to write block %s to target %s: %v", blockID, targetNode.ID, err)
		nameNode.metaLock.Lock() // Re-acquire lock before returning
		return
	}
	nameNode.metaLock.Lock() // Re-acquire lock to update metadata

	// 4. Update metadata
	nameNode.DataNodeToBlockMapping[targetNode.ID] = append(nameNode.DataNodeToBlockMapping[targetNode.ID], blockID)
	log.Printf("✅ Successfully re-replicated block %s to %s", blockID, targetNode.ID)
	nameNode.persistMetadataToJSON(MetadataFile)
}

// readBlockFromDataNode connects to a DataNode and reads a block
func (nameNode *NameNodeData) readBlockFromDataNode(dn DataNodeMetadata, blockID string) ([]byte, error) {
	connStr := net.JoinHostPort(dn.Host, dn.Port)
	conn, err := grpc.Dial(connStr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := datanodeService.NewDatanodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := client.ReadBytesFromDataNode(ctx, &datanodeService.BlockRequest{BlockID: blockID})
	if err != nil {
		return nil, err
	}
	return res.FileContent, nil
}

// writeBlockToDataNode connects to a DataNode and writes a block
func (nameNode *NameNodeData) writeBlockToDataNode(dn DataNodeMetadata, blockID string, content []byte) error {
	connStr := net.JoinHostPort(dn.Host, dn.Port)
	conn, err := grpc.Dial(connStr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := datanodeService.NewDatanodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Compute checksum before sending
	blockChecksum := checksum.ComputeChecksum(content)

	_, err = client.SendDataToDataNodes(ctx, &datanodeService.ClientToDataNodeRequest{
		BlockID:  blockID,
		Content:  content,
		Checksum: blockChecksum, // Add checksum field
	})
	return err // ✅ Return goes HERE, after the function call
}

func (nameNode *NameNodeData) DeleteFile(ctx context.Context, fileData *namenode.FileData) (*namenode.Status, error) {
	nameNode.metaLock.Lock()
	defer nameNode.metaLock.Unlock()

	fileName := fileData.FileName

	// Check if file exists
	blockIDs, exists := nameNode.FileToBlockMapping[fileName]
	if !exists {
		return &namenode.Status{StatusMessage: "File not found"}, errors.New("file does not exist")
	}

	// Remove file-to-block mapping
	delete(nameNode.FileToBlockMapping, fileName)

	// Remove blocks from datanode mappings
	for _, blockID := range blockIDs {
		for dnID, blocks := range nameNode.DataNodeToBlockMapping {
			// Remove blockID from this datanode's list
			newBlocks := []string{}
			for _, b := range blocks {
				if b != blockID {
					newBlocks = append(newBlocks, b)
				}
			}
			nameNode.DataNodeToBlockMapping[dnID] = newBlocks
		}
	}

	// Persist metadata
	nameNode.persistMetadataToJSON(MetadataFile)

	log.Printf("✅ Deleted file: %s (%d blocks)", fileName, len(blockIDs))
	return &namenode.Status{StatusMessage: "File deleted successfully"}, nil
}

func (nameNode *NameNodeData) ReportCorruptBlock(ctx context.Context, report *namenode.CorruptBlockReport) (*namenode.Status, error) {
	nameNode.metaLock.Lock()
	defer nameNode.metaLock.Unlock()

	blockID := report.BlockID
	corruptNodeID := report.DatanodeID
	reason := report.Reason

	log.Printf("🚨 CORRUPT BLOCK REPORT: Block %s on DataNode %s - Reason: %s",
		blockID, corruptNodeID, reason)

	// Remove the corrupt block from the datanode's block list
	if blocks, ok := nameNode.DataNodeToBlockMapping[corruptNodeID]; ok {
		newBlocks := []string{}
		for _, b := range blocks {
			if b != blockID {
				newBlocks = append(newBlocks, b)
			}
		}
		nameNode.DataNodeToBlockMapping[corruptNodeID] = newBlocks
		log.Printf("Removed corrupt block %s from DataNode %s mapping", blockID, corruptNodeID)
	}

	// Persist the change
	nameNode.persistMetadataToJSON(MetadataFile)

	// Trigger re-replication in the background
	go func() {
		log.Printf("Triggering re-replication for corrupt block %s", blockID)
		nameNode.reReplicateBlock(blockID, corruptNodeID)
	}()

	return &namenode.Status{StatusMessage: "Corrupt block reported and re-replication triggered"}, nil
}
