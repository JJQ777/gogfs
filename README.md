#  MiniGoDFS

    A distributed file system inspired by HDFS built in Golang and gRPC. It is designed to provide high availability, scalability, and fault tolerance for file storage across multiple nodes. GoDFS uses gRPC for fast and efficient communication between nodes.

## Features
- **Distributed Architecture**: Store files across multiple nodes to ensure high availability and redundancy.
- **Fault Tolerance**: Automatically replicate files across nodes to prevent data loss in case of node failures.
- **High Performance**: Optimized for fast file operations and low latency using gRPC for communication

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

- Run the namenode: `make run-namenode`
- Run the datanodes: `make run-datanodes`
- Run the client write: `make run-client-write`
- Run the client read:  `make run-client-read`





