GOCMD = "go"
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test

BINARY_NAME=go-dfs

build:
	$(GOBUILD) -o $(BINARY_NAME) -v main.go

run:
	$(GOBUILD) -o $(BINARY_NAME) -v main.go

run-namenode:
	make run
	./$(BINARY_NAME) namenode -port 8080 -block-size 32 -role active -peer localhost:8081

run-namenode-standby:
	make run
	./$(BINARY_NAME) namenode -port 8081 -block-size 32 -role standby -peer localhost:8080

run-client-write:
	make run
	./$(BINARY_NAME) client -namenode 8080 -operation write -source-path . -filename big.txt

run-client-read:
	make run
	./$(BINARY_NAME) client -namenode 8080 -operation read -source-path . -filename big.txt

# HotBackup testing targets - connect to Standby NameNode (port 8081)
run-client-write-standby:
	make run
	./$(BINARY_NAME) client -namenode 8081 -operation write -source-path . -filename big.txt

run-client-read-standby:
	make run
	./$(BINARY_NAME) client -namenode 8081 -operation read -source-path . -filename big.txt

run-datanodes:
	make run
	bash scripts/run_datanodes.sh
deps:
	$(GOGET) -v ./..

protoc: 
	bash scripts/generate_proto.sh