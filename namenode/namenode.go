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

	namenode "github.com/JJQ777/gogfs/proto/namenode"
	"github.com/JJQ777/gogfs/utils"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
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
	log.Printf("Namenode is listening on port %s\n", address)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// persistance
func (nameNode *NameNodeData) persistMetadataToJSON(filename string) {
	nameNode.metaLock.Lock()
	defer nameNode.metaLock.Unlock()

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
func (nameNode *NameNodeData) Register_DataNode(ctx context.Context, datanodeData *namenode.DatanodeData) (status *namenode.Status, err error) {
	nameNode.metaLock.Lock()
	defer nameNode.metaLock.Unlock()

	log.Printf("%s %d\n", datanodeData.DatanodeID, nameNode.BlockSize)
	_, ok := nameNode.DataNodeToBlockMapping[datanodeData.DatanodeID]
	if !ok {
		nameNode.DataNodeToBlockMapping[datanodeData.DatanodeID] = make([]string, 0)
		dnmetadata := DataNodeMetadata{ID: datanodeData.DatanodeID, Port: datanodeData.DatanodePort, Status: "Available"}
		nameNode.DataNodeToMetadataMapping[datanodeData.DatanodeID] = dnmetadata
		nameNode.persistMetadataToJSON(MetadataFile)
		return &namenode.Status{StatusMessage: "Registered"}, nil
	}
	return &namenode.Status{StatusMessage: "Exists"}, nil

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
	defer nameNode.metaLock.Unlock()

	nameNode.DataNodeToBlockMapping[dataNodeBlockData.DatanodeID] = dataNodeBlockData.Blocks
	nameNode.persistMetadataToJSON(MetadataFile)
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
	defer nameNode.metaLock.Unlock()

	filePath := fileBlockMetadata.FilePath
	blockIDs := fileBlockMetadata.BlockIDs

	nameNode.FileToBlockMapping[filePath] = blockIDs
	nameNode.persistMetadataToJSON(MetadataFile)
	return &namenode.Status{StatusMessage: "Success"}, nil

}
