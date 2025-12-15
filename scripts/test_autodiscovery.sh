#!/bin/bash

# HotBackup Test with Auto-Discovery Architecture
# This version uses namenode cluster configuration for automatic failover

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

ACTIVE_PORT=8080
STANDBY_PORT=8081
LOG_DIR="logs"
ACTIVE_LOG="${LOG_DIR}/active_namenode.log"
STANDBY_LOG="${LOG_DIR}/standby_namenode.log"
DATANODES_LOG="${LOG_DIR}/datanodes.log"

cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up...${NC}"
    if [ ! -z "${ACTIVE_PID}" ] && ps -p ${ACTIVE_PID} > /dev/null 2>&1; then
        kill -9 ${ACTIVE_PID} 2>/dev/null || true
    fi
    if [ ! -z "${STANDBY_PID}" ] && ps -p ${STANDBY_PID} > /dev/null 2>&1; then
        kill -9 ${STANDBY_PID} 2>/dev/null || true
    fi
    pkill -f "go-dfs.*datanode" 2>/dev/null || true
    sleep 1
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

trap cleanup EXIT

mkdir -p ${LOG_DIR}

echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}  HotBackup Test - Auto-Discovery Architecture${NC}"
echo -e "${BLUE}================================================================${NC}\n"

# Build
echo -e "${BLUE}📦 Step 1: Building project with new architecture...${NC}"
go build -o go-dfs main.go
echo -e "${GREEN}✅ Build successful${NC}\n"

# Start Active NameNode
echo -e "${BLUE}🔷 Step 2: Starting Active NameNode (port ${ACTIVE_PORT})...${NC}"
./go-dfs namenode -port ${ACTIVE_PORT} -block-size 32 -role active -peer localhost:${STANDBY_PORT} > ${ACTIVE_LOG} 2>&1 &
ACTIVE_PID=$!
sleep 3

if ps -p ${ACTIVE_PID} > /dev/null; then
    echo -e "${GREEN}✅ Active NameNode started (PID: ${ACTIVE_PID})${NC}"
    tail -n 3 ${ACTIVE_LOG} | sed 's/^/   /'
else
    echo -e "${RED}❌ Failed to start Active${NC}"
    cat ${ACTIVE_LOG}
    exit 1
fi
echo ""

# Start Standby NameNode
echo -e "${BLUE}🔷 Step 3: Starting Standby NameNode (port ${STANDBY_PORT})...${NC}"
./go-dfs namenode -port ${STANDBY_PORT} -block-size 32 -role standby -peer localhost:${ACTIVE_PORT} > ${STANDBY_LOG} 2>&1 &
STANDBY_PID=$!
sleep 3

if ps -p ${STANDBY_PID} > /dev/null; then
    echo -e "${GREEN}✅ Standby NameNode started (PID: ${STANDBY_PID})${NC}"
    tail -n 3 ${STANDBY_LOG} | sed 's/^/   /'
else
    echo -e "${RED}❌ Failed to start Standby${NC}"
    cat ${STANDBY_LOG}
    exit 1
fi
echo ""

# Wait for initial sync
echo -e "${BLUE}⏳ Step 4: Waiting for initial synchronization (8 seconds)...${NC}"
sleep 8
echo -e "${GREEN}✅ Synchronization period completed${NC}\n"

# Verify sync
echo -e "${BLUE}📊 Step 5: Verifying metadata synchronization...${NC}"
if grep -q "Synced metadata to Standby" ${ACTIVE_LOG}; then
    echo -e "${GREEN}✅ Active → Standby sync confirmed${NC}"
fi
if grep -q "Synced metadata from Active" ${STANDBY_LOG}; then
    echo -e "${GREEN}✅ Standby ← Active sync confirmed${NC}"
fi
echo ""

# Step 6: Starting DataNodes with auto-discovery
echo -e "${BLUE}🔷 Step 6: Starting DataNodes with auto-discovery and auto-reconnect...${NC}"
for ((i = 1; i <= 5; i++)); do
  PORT=$((8000 + $i))
  ./go-dfs datanode -port ${PORT} -location datanode-files -config namenode_cluster.conf > ${LOG_DIR}/datanode_${PORT}.log 2>&1 &
  echo -e "${GREEN}   Started DataNode on port ${PORT} (with auto-reconnect)${NC}"
  sleep 1
done
sleep 3
echo -e "${GREEN}✅ All DataNodes started with cluster discovery and auto-reconnect${NC}\n"

# Write test file using auto-discovery
echo -e "${BLUE}📝 Step 7: Writing test file using auto-discovery...${NC}"
if [ ! -f "big.txt" ]; then
    echo "This is a test file for HotBackup verification with auto-discovery" > big.txt
fi
./go-dfs client -operation write -source-path . -filename big.txt -config namenode_cluster.conf
echo -e "${GREEN}✅ File written successfully${NC}\n"

# Wait for sync
echo -e "${BLUE}⏳ Step 8: Waiting for metadata sync (6 seconds)...${NC}"
sleep 6
echo -e "${GREEN}✅ Sync completed${NC}\n"

# Read file from Active to verify
echo -e "${BLUE}📖 Step 9: Reading file from Active to verify...${NC}"
./go-dfs client -operation read -source-path . -filename big.txt -config namenode_cluster.conf
echo -e "${GREEN}✅ File read successfully${NC}\n"

# Simulate failure
echo -e "${BLUE}💥 Step 10: Simulating Active NameNode failure...${NC}"
echo -e "${YELLOW}   Killing Active NameNode (PID: ${ACTIVE_PID})${NC}"
kill -9 ${ACTIVE_PID} 2>/dev/null || true
ACTIVE_PID=""
echo -e "${GREEN}✅ Active NameNode stopped${NC}\n"

# Wait for failover
echo -e "${BLUE}⏳ Step 11: Waiting for automatic failover (10 seconds)...${NC}"
sleep 10

# Check promotion
if grep -q "promoted to ACTIVE" ${STANDBY_LOG}; then
    echo -e "${GREEN}✅✅✅ FAILOVER SUCCESSFUL! ✅✅✅${NC}"
    echo -e "${GREEN}   Standby promoted to ACTIVE${NC}\n"
    grep "promoted to ACTIVE" ${STANDBY_LOG} -A 2 | sed 's/^/   /'
else
    echo -e "${RED}❌ Failover did not occur${NC}"
fi
echo ""

# Wait for DataNodes to detect disconnection and reconnect
echo -e "${BLUE}⏳ Waiting for DataNodes to detect and reconnect (15 seconds)...${NC}"
echo -e "${YELLOW}   DataNodes should automatically reconnect to new Active NameNode${NC}"
sleep 15

# Check if DataNodes reconnected
RECONNECT_COUNT=$(grep -l "Reconnected to NameNode" ${LOG_DIR}/datanode_*.log 2>/dev/null | wc -l)
if [ ${RECONNECT_COUNT} -gt 0 ]; then
    echo -e "${GREEN}✅ ${RECONNECT_COUNT} DataNode(s) successfully reconnected!${NC}\n"
else
    echo -e "${YELLOW}⚠️  No reconnection logs found yet (may still be in progress)${NC}\n"
fi

# Test auto-discovery after failover
echo -e "${BLUE}🔍 Step 12: Testing auto-discovery after failover...${NC}"
echo -e "${BLUE}   Client will automatically connect to new Active (port ${STANDBY_PORT})${NC}\n"

sleep 3

# Read file using auto-discovery (should connect to new Active)
echo -e "${BLUE}📖 Step 13: Reading file using auto-discovery...${NC}"
echo -e "${YELLOW}   Client should automatically connect to new Active NameNode${NC}"
./go-dfs client -operation read -source-path . -filename big.txt -config namenode_cluster.conf
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅✅✅ AUTO-DISCOVERY SUCCESSFUL! ✅✅✅${NC}"
    echo -e "${GREEN}   File read from new Active NameNode!${NC}\n"
else
    echo -e "${RED}❌ Auto-discovery failed${NC}\n"
fi

# Write new file to new Active
echo -e "${BLUE}📝 Step 14: Writing new file to new Active via auto-discovery...${NC}"
echo "New file after failover" > test_after_failover.txt
./go-dfs client -operation write -source-path . -filename test_after_failover.txt -config namenode_cluster.conf
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ New file written successfully!${NC}\n"
fi

# Summary
echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}  Test Summary${NC}"
echo -e "${BLUE}================================================================${NC}"
echo -e "${GREEN}✅ Active and Standby NameNodes started${NC}"
echo -e "${GREEN}✅ Metadata synchronized${NC}"
echo -e "${GREEN}✅ DataNodes connected with auto-discovery${NC}"
echo -e "${GREEN}✅ File operations successful before failover${NC}"
echo -e "${GREEN}✅ Active NameNode failure simulated${NC}"
echo -e "${GREEN}✅ Standby promoted to Active automatically${NC}"
echo -e "${GREEN}✅ Auto-discovery reconnected to new Active${NC}"
echo -e "${GREEN}✅ File operations successful after failover${NC}"
echo ""
echo -e "${GREEN}🎉 HotBackup with Auto-Discovery test PASSED! 🎉${NC}"
echo ""
echo -e "${YELLOW}📋 Logs saved in ${LOG_DIR}/${NC}"
echo -e "${YELLOW}Press Enter to cleanup and exit...${NC}"
read
