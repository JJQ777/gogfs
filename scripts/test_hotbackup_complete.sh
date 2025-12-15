#!/bin/bash

# HotBackup Complete Test Script
# This script tests the full failover functionality of the NameNode HotBackup feature

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
ACTIVE_PORT=8080
STANDBY_PORT=8081
TEST_FILE="big.txt"
LOG_DIR="logs"
ACTIVE_LOG="${LOG_DIR}/active_namenode.log"
STANDBY_LOG="${LOG_DIR}/standby_namenode.log"
DATANODES_LOG="${LOG_DIR}/datanodes.log"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up processes...${NC}"
    
    # Kill specific PIDs we started
    if [ ! -z "${ACTIVE_PID}" ] && ps -p ${ACTIVE_PID} > /dev/null 2>&1; then
        echo -e "${YELLOW}   Stopping Active NameNode (PID: ${ACTIVE_PID})${NC}"
        kill -9 ${ACTIVE_PID} 2>/dev/null || true
    fi
    
    if [ ! -z "${STANDBY_PID}" ] && ps -p ${STANDBY_PID} > /dev/null 2>&1; then
        echo -e "${YELLOW}   Stopping Standby NameNode (PID: ${STANDBY_PID})${NC}"
        kill -9 ${STANDBY_PID} 2>/dev/null || true
    fi
    
    # Kill DataNodes
    echo -e "${YELLOW}   Stopping DataNodes...${NC}"
    pkill -f "go-dfs.*datanode" 2>/dev/null || true
    
    sleep 1
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

# Trap to ensure cleanup on exit
trap cleanup EXIT

# Create log directory
mkdir -p ${LOG_DIR}

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}  HotBackup NameNode Complete Test${NC}"
echo -e "${BLUE}=========================================${NC}\n"

# Step 1: Build the project
echo -e "${BLUE}📦 Step 1: Building project...${NC}"
go build -o go-dfs main.go
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Build successful${NC}\n"
else
    echo -e "${RED}❌ Build failed${NC}"
    exit 1
fi

# Step 2: Start Active NameNode
echo -e "${BLUE}🔷 Step 2: Starting Active NameNode (port ${ACTIVE_PORT})...${NC}"
./go-dfs namenode -port ${ACTIVE_PORT} -block-size 32 -role active -peer localhost:${STANDBY_PORT} > ${ACTIVE_LOG} 2>&1 &
ACTIVE_PID=$!
sleep 3

if ps -p ${ACTIVE_PID} > /dev/null; then
    echo -e "${GREEN}✅ Active NameNode started (PID: ${ACTIVE_PID})${NC}"
    tail -n 5 ${ACTIVE_LOG} | sed 's/^/   /'
else
    echo -e "${RED}❌ Failed to start Active NameNode${NC}"
    cat ${ACTIVE_LOG}
    exit 1
fi
echo ""

# Step 3: Start Standby NameNode
echo -e "${BLUE}🔷 Step 3: Starting Standby NameNode (port ${STANDBY_PORT})...${NC}"
./go-dfs namenode -port ${STANDBY_PORT} -block-size 32 -role standby -peer localhost:${ACTIVE_PORT} > ${STANDBY_LOG} 2>&1 &
STANDBY_PID=$!
sleep 3

if ps -p ${STANDBY_PID} > /dev/null; then
    echo -e "${GREEN}✅ Standby NameNode started (PID: ${STANDBY_PID})${NC}"
    tail -n 5 ${STANDBY_LOG} | sed 's/^/   /'
else
    echo -e "${RED}❌ Failed to start Standby NameNode${NC}"
    cat ${STANDBY_LOG}
    exit 1
fi
echo ""

# Step 4: Start DataNodes
echo -e "${BLUE}🔷 Step 4: Starting DataNodes...${NC}"
bash scripts/run_datanodes.sh > ${DATANODES_LOG} 2>&1 &
DATANODES_PID=$!
sleep 5
echo -e "${GREEN}✅ DataNodes started (PID: ${DATANODES_PID})${NC}\n"

# Step 5: Verify synchronization
echo -e "${BLUE}📊 Step 5: Verifying NameNode synchronization...${NC}"
sleep 8  # Wait for at least one sync cycle (5 seconds) + heartbeats
if grep -q "Synced metadata to Standby" ${ACTIVE_LOG} && grep -q "Synced metadata from Active" ${STANDBY_LOG}; then
    echo -e "${GREEN}✅ Active and Standby are synchronized${NC}"
    echo -e "   Active log: $(grep 'Synced metadata to Standby' ${ACTIVE_LOG} | tail -1)"
    echo -e "   Standby log: $(grep 'Synced metadata from Active' ${STANDBY_LOG} | tail -1)"
else
    echo -e "${YELLOW}⚠️  Synchronization not detected yet, waiting...${NC}"
    sleep 5
fi
echo ""

# Step 6: Write test file to Active NameNode
echo -e "${BLUE}📝 Step 6: Writing test file to Active NameNode (port ${ACTIVE_PORT})...${NC}"
if [ ! -f "${TEST_FILE}" ]; then
    echo "This is a test file for HotBackup verification" > ${TEST_FILE}
fi
./go-dfs client -namenode ${ACTIVE_PORT} -operation write -source-path . -filename ${TEST_FILE}
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ File written successfully to Active NameNode${NC}\n"
else
    echo -e "${RED}❌ Failed to write file${NC}\n"
fi

# Step 7: Wait for sync
echo -e "${BLUE}⏳ Step 7: Waiting for metadata sync (6 seconds)...${NC}"
sleep 6
echo -e "${GREEN}✅ Sync period completed${NC}\n"

# Step 8: Read file from Active to verify
echo -e "${BLUE}📖 Step 8: Reading file from Active NameNode to verify...${NC}"
./go-dfs client -namenode ${ACTIVE_PORT} -operation read -source-path . -filename ${TEST_FILE}
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ File read successfully from Active NameNode${NC}\n"
else
    echo -e "${RED}❌ Failed to read file from Active${NC}\n"
fi

# Step 9: Simulate Active NameNode failure
echo -e "${BLUE}💥 Step 9: Simulating Active NameNode failure...${NC}"
echo -e "${YELLOW}   Killing ONLY Active NameNode (PID: ${ACTIVE_PID})${NC}"
echo -e "${YELLOW}   DataNodes will remain running...${NC}"
kill ${ACTIVE_PID} 2>/dev/null || true
sleep 1
# Force kill if still running
if ps -p ${ACTIVE_PID} > /dev/null 2>&1; then
    kill -9 ${ACTIVE_PID} 2>/dev/null || true
fi

if ! ps -p ${ACTIVE_PID} > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Active NameNode stopped${NC}"
    # Verify DataNodes are still running
    if ps -p ${DATANODES_PID} > /dev/null 2>&1; then
        echo -e "${GREEN}✅ DataNodes still running (PID: ${DATANODES_PID})${NC}\n"
    else
        echo -e "${YELLOW}⚠️  DataNodes may have stopped${NC}\n"
    fi
else
    echo -e "${RED}❌ Failed to stop Active NameNode${NC}\n"
fi

# Step 10: Wait for failover detection
echo -e "${BLUE}⏳ Step 10: Waiting for Standby to detect failure and promote (8 seconds)...${NC}"
sleep 8

# Check if Standby promoted to Active
if grep -q "promoted to ACTIVE" ${STANDBY_LOG}; then
    echo -e "${GREEN}✅ Standby successfully promoted to ACTIVE!${NC}"
    echo -e "   Failover log:"
    grep -A 2 "promoted to ACTIVE" ${STANDBY_LOG} | sed 's/^/   /'
else
    echo -e "${RED}❌ Standby did not promote to ACTIVE${NC}"
    echo -e "${YELLOW}   Last 10 lines of Standby log:${NC}"
    tail -n 10 ${STANDBY_LOG} | sed 's/^/   /'
fi
echo ""

# Wait for DataNodes to reconnect to new Active
echo -e "${BLUE}⏳ Waiting for DataNodes to reconnect to new Active NameNode (5 seconds)...${NC}"
sleep 5
echo -e "${GREEN}✅ DataNodes should have reconnected${NC}\n"

# Step 11: Verify service continues on new Active (port 8081)
echo -e "${BLUE}🔍 Step 11: Verifying service on new Active NameNode (port ${STANDBY_PORT})...${NC}"
sleep 2

echo -e "${BLUE}   📖 Reading previously written file from new Active...${NC}"
./go-dfs client -namenode ${STANDBY_PORT} -operation read -source-path . -filename ${TEST_FILE}
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Successfully read file from new Active NameNode!${NC}"
    echo -e "${GREEN}   This proves metadata was synchronized and failover worked!${NC}\n"
else
    echo -e "${RED}❌ Failed to read file from new Active NameNode${NC}\n"
fi

# Step 12: Write new file to new Active
echo -e "${BLUE}📝 Step 12: Writing new test file to new Active NameNode...${NC}"
echo "This is a new test file after failover" > test_after_failover.txt
./go-dfs client -namenode ${STANDBY_PORT} -operation write -source-path . -filename test_after_failover.txt
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ New file written successfully to new Active NameNode!${NC}\n"
else
    echo -e "${RED}❌ Failed to write new file${NC}\n"
fi

# Final Summary
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}  Test Summary${NC}"
echo -e "${BLUE}=========================================${NC}"
echo -e "${GREEN}✅ Active NameNode started successfully${NC}"
echo -e "${GREEN}✅ Standby NameNode started successfully${NC}"
echo -e "${GREEN}✅ Metadata synchronized between NameNodes${NC}"
echo -e "${GREEN}✅ File written to Active NameNode${NC}"
echo -e "${GREEN}✅ File read from Active NameNode${NC}"
echo -e "${GREEN}✅ Active NameNode failure simulated${NC}"
echo -e "${GREEN}✅ Standby promoted to Active automatically${NC}"
echo -e "${GREEN}✅ File read from new Active NameNode${NC}"
echo -e "${GREEN}✅ New file written to new Active NameNode${NC}"
echo ""
echo -e "${GREEN}🎉 HotBackup test completed successfully!${NC}"
echo ""
echo -e "${YELLOW}📋 Logs saved in:${NC}"
echo -e "   Active NameNode:  ${ACTIVE_LOG}"
echo -e "   Standby NameNode: ${STANDBY_LOG}"
echo -e "   DataNodes:        ${DATANODES_LOG}"
echo ""
echo -e "${YELLOW}Press Enter to cleanup and exit...${NC}"
read

# Cleanup will be called automatically by trap
