#  MiniGoDFS

    A distributed file system inspired by HDFS built in Golang and gRPC. It is designed to provide high availability, scalability, and fault tolerance for file storage across multiple nodes. GoDFS uses gRPC for fast and efficient communication between nodes.

## Features
- **Distributed Architecture**: Store files across multiple nodes to ensure high availability and redundancy.
- **Fault Tolerance**: Automatically replicate files across nodes to prevent data loss in case of node failures.
- **High Performance**: Optimized for fast file operations and low latency using gRPC for communication
- **Persistence Support**: Metadata is persisted to disk and automatically recovered on restart
- **HotBackup (Standby NameNode)**: Active/Standby NameNode configuration with automatic failover for high availability
- **Auto-Discovery Architecture**: 🆕 DataNodes and Clients automatically discover and connect to available Active NameNode using cluster configuration

## Components
There are three main components of GoDFS:
- **Namenode** : Responsible for storing all the metadata of the files in miniGoDFS and acts as the brain of the system
- **Datanode** : Responsible for storing the file data in chunks and forwarding chunks to other datanodes
- **Client** : Responsible for handling the read and write requests for files


## Quick Start
Please run this project in Linux.(Running on Windows may result in errors)

Pull the repository: `git clone https://github.com/JJQ777/gogfs.git`

Generate .go files from .proto files:
```
cd gogfs
make protoc
```
if the following error occur
```
bash scripts/generate_proto.sh
protoc-gen-go: program not found or is not executable
Please specify a program using absolute path or make sure the program is available in your PATH system variable
--go_out: protoc-gen-go: Plugin failed with status code 1.
protoc-gen-go: program not found or is not executable
Please specify a program using absolute path or make sure the program is available in your PATH system variable
--go_out: protoc-gen-go: Plugin failed with status code 1.
make: *** [Makefile:30: protoc] Error 1
```
you can run below codes in same terminal to fix:
```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
export PATH="$PATH:$(go env GOPATH)/bin"
source ~/.bashrc   # or ~/.zshrc
```
then rerun `make protoc`

Please ensure that the path does not contain any spaces before run following command to test.

### Standard Mode (Single NameNode)
- Run the namenode: `make run-namenode`
- Run the datanodes: `make run-datanodes`
- Run the client write: `make run-client-write`
- Run the client read:  `make run-client-read`

After run the client write successfully, you can close the minigodfs and restart to read, the command will run successfully because of the persistance support.

### HotBackup Mode (Active/Standby NameNodes) 🆕

GoDFS now supports **High Availability** with a Standby NameNode that automatically takes over if the Active NameNode fails.

#### Starting NameNodes:
- Run the Active NameNode: `make run-namenode` (port 8080)
- Run the Standby NameNode: `make run-namenode-standby` (port 8081)
- Run the datanodes: `make run-datanodes`

#### Testing Failover:
```bash
bash scripts/test_hotbackup.sh
```

**For detailed information about the HotBackup feature, see [HOTBACKUP.md](HOTBACKUP.md)**

### Auto-Discovery Mode (Recommended) 🆕🔥

The newest architecture supports **automatic NameNode discovery** for seamless failover. DataNodes and Clients automatically find and connect to the available Active NameNode.

#### Configuration:
Edit `namenode_cluster.conf`:
```
namenodes=localhost:8080,localhost:8081
primary=localhost:8080
```

#### Starting with Auto-Discovery:
```bash
# Start NameNodes
make run-namenode          # Active on 8080
make run-namenode-standby  # Standby on 8081

# Start DataNodes with auto-discovery
./go-dfs datanode -port 8001 -location datanode-files -config namenode_cluster.conf

# Client operations with auto-discovery
./go-dfs client -operation write -filename test.txt -source-path . -config namenode_cluster.conf
./go-dfs client -operation read -filename test.txt -source-path . -config namenode_cluster.conf
```

#### Testing Auto-Discovery and Failover:
```bash
bash scripts/test_autodiscovery.sh
```

**Benefits:**
- ✅ Automatic failover without manual intervention
- ✅ Clients automatically reconnect to new Active NameNode
- ✅ DataNodes work seamlessly across failover events
- ✅ No hardcoded ports or addresses

**For detailed information about the Auto-Discovery architecture, see [AUTODISCOVERY.md](AUTODISCOVERY.md)**



