package main

import (
	"flag"
	"log"
	"os"

	"github.com/JJQ777/gogfs/client"
	"github.com/JJQ777/gogfs/datanode"
	"github.com/JJQ777/gogfs/namenode"
	"github.com/JJQ777/gogfs/utils"
	"google.golang.org/grpc"
)

func main() {

	dataNodeCommand := flag.NewFlagSet("datanode", flag.ExitOnError)
	nameNodeCommand := flag.NewFlagSet("namenode", flag.ExitOnError)
	clientCommand := flag.NewFlagSet("client", flag.ExitOnError)

	dataNodePortPtr := dataNodeCommand.String("port", "8081", "Port of datanode")
	dataNodeLocationPtr := dataNodeCommand.String("location", ".", "Location of files to be stored by datanode")
	dataNodeConfigPtr := dataNodeCommand.String("config", "namenode_cluster.conf", "Path to namenode cluster config file")

	nameNodePortPtr := nameNodeCommand.String("port", "8080", "Port of namenode")
	nameNodeBlockSizePtr := nameNodeCommand.Int64("block-size", 32, "Block size to store")
	nameNodeRolePtr := nameNodeCommand.String("role", "active", "NameNode role: active or standby")
	nameNodePeerPtr := nameNodeCommand.String("peer", "", "Peer NameNode address (e.g., localhost:8081)")

	//clientPortPtr := clientCommand.String("port", "8080", "Port of client")
	clientNameNodePortPtr := clientCommand.String("namenode", *nameNodePortPtr, "NameNode communication port")
	clientOperationPtr := clientCommand.String("operation", "", "Operation to perform")
	clientSourcePathPtr := clientCommand.String("source-path", ".", "Source path of the file")
	clientFilenamePtr := clientCommand.String("filename", "", "File name")
	clientConfigPtr := clientCommand.String("config", "namenode_cluster.conf", "Path to namenode cluster config file")
	//clientReadFilenamePtr := clientCommand.String("read-file", "", "File name to read")

	if len(os.Args) < 2 {
		log.Println("sub-command is required")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {

	case "namenode":
		_ = nameNodeCommand.Parse(os.Args[2:])
		namenodePtr := &namenode.NameNodeData{}
		namenodePtr.InitializeNameNode(*nameNodePortPtr, *nameNodeBlockSizePtr, *nameNodeRolePtr, *nameNodePeerPtr)
	case "datanode":
		_ = dataNodeCommand.Parse(os.Args[2:])
		datanodePtr := &datanode.DataNode{}
		datanodePtr.InitializeDataNode(*dataNodePortPtr, *dataNodeLocationPtr)
		datanodePtr.Port = *dataNodePortPtr
		datanodePtr.ConfigPath = *dataNodeConfigPtr
		
		// Use cluster discovery for initial connection
		config, err := utils.LoadClusterConfig(*dataNodeConfigPtr)
		if err != nil {
			log.Printf("⚠️  Failed to load cluster config, using default port")
			conn := datanodePtr.ConnectToNameNode(*nameNodePortPtr, "localhost")
			datanodePtr.ActiveConn = conn
			datanodePtr.RegisterNode(conn, *dataNodePortPtr)
			go datanodePtr.SendBlockReportToNameNode(conn)
		} else {
			conn, activeAddr, err := utils.DiscoverActiveNamenode(config)
			if err != nil {
				log.Fatalf("❌ Failed to discover active NameNode: %v", err)
			}
			log.Printf("🔗 DataNode connected to NameNode at %s", activeAddr)
			datanodePtr.ActiveConn = conn
			datanodePtr.RegisterNode(conn, *dataNodePortPtr)
			go datanodePtr.SendBlockReportToNameNode(conn)
			
			// Start connection monitor for auto-reconnect
			go datanodePtr.MonitorConnectionAndReconnect()
			log.Printf("🔄 Auto-reconnect monitoring started")
		}
		
		datanodePtr.StartServer(*dataNodePortPtr)
	case "client":
		_ = clientCommand.Parse(os.Args[2:])
		clientPtr := &client.ClientData{}
		
		// Try to use cluster discovery first
		config, err := utils.LoadClusterConfig(*clientConfigPtr)
		var conn *grpc.ClientConn
		
		if err == nil {
			// Use discovery
			var activeAddr string
			conn, activeAddr, err = utils.DiscoverActiveNamenode(config)
			if err != nil {
				log.Fatalf("❌ Failed to discover active NameNode: %v", err)
			}
			log.Printf("🔗 Client connected to NameNode at %s", activeAddr)
			clientPtr.NameNodePort = activeAddr
		} else {
			// Fallback to direct connection
			clientPtr.InitializeClient(*clientNameNodePortPtr)
			conn = clientPtr.ConnectToNameNode()
		}
		
		if *clientOperationPtr == "write" {
			clientPtr.WriteFile(conn, *clientSourcePathPtr, *clientFilenamePtr)
		}
		if *clientOperationPtr == "read" {
			clientPtr.ReadFile(conn, *clientSourcePathPtr, *clientFilenamePtr)
		}

	}

}
