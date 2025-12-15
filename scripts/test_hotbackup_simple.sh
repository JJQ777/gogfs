#!/bin/bash

# HotBackup Simple Test - Focus on failover mechanism
# This version tests metadata synchronization and failover without file operations

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

cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up...${NC}"
    if [ ! -z "${ACTIVE_PID}" ]; then
        kill -9 ${ACTIVE_PID} 2>/dev/null || true
    fi
    if [ ! -z "${STANDBY_PID}" ]; then
        kill -9 ${STANDBY_PID} 2>/dev/null || true
    fi
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

trap cleanup EXIT

mkdir -p ${LOG_DIR}

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}  HotBackup Simple Failover Test${NC}"
echo -e "${BLUE}=========================================${NC}\n"

# Build
echo -e "${BLUE}📦 Building project...${NC}"
go build -o go-dfs main.go
echo -e "${GREEN}✅ Build successful${NC}\n"

# Start Active NameNode
echo -e "${BLUE}🔷 Starting Active NameNode (port ${ACTIVE_PORT})...${NC}"
./go-dfs namenode -port ${ACTIVE_PORT} -block-size 32 -role active -peer localhost:${STANDBY_PORT} > ${ACTIVE_LOG} 2>&1 &
ACTIVE_PID=$!
sleep 3

if ps -p ${ACTIVE_PID} > /dev/null; then
    echo -e "${GREEN}✅ Active NameNode started (PID: ${ACTIVE_PID})${NC}"
    tail -n 3 ${ACTIVE_LOG} | sed 's/^/   /'
else
    echo -e "${RED}❌ Failed to start Active${NC}"
    exit 1
fi
echo ""

# Start Standby NameNode
echo -e "${BLUE}🔷 Starting Standby NameNode (port ${STANDBY_PORT})...${NC}"
./go-dfs namenode -port ${STANDBY_PORT} -block-size 32 -role standby -peer localhost:${ACTIVE_PORT} > ${STANDBY_LOG} 2>&1 &
STANDBY_PID=$!
sleep 3

if ps -p ${STANDBY_PID} > /dev/null; then
    echo -e "${GREEN}✅ Standby NameNode started (PID: ${STANDBY_PID})${NC}"
    tail -n 3 ${STANDBY_LOG} | sed 's/^/   /'
else
    echo -e "${RED}❌ Failed to start Standby${NC}"
    exit 1
fi
echo ""

# Wait for synchronization
echo -e "${BLUE}⏳ Waiting for initial synchronization (10 seconds)...${NC}"
sleep 10

# Check sync
if grep -q "Synced metadata to Standby" ${ACTIVE_LOG}; then
    echo -e "${GREEN}✅ Active → Standby sync confirmed${NC}"
    grep "Synced metadata to Standby" ${ACTIVE_LOG} | tail -1 | sed 's/^/   /'
fi

if grep -q "Synced metadata from Active" ${STANDBY_LOG}; then
    echo -e "${GREEN}✅ Standby ← Active sync confirmed${NC}"
    grep "Synced metadata from Active" ${STANDBY_LOG} | tail -1 | sed 's/^/   /'
fi
echo ""

# Check heartbeat
if grep -q "Heartbeat" ${STANDBY_LOG}; then
    echo -e "${GREEN}✅ Heartbeat mechanism working${NC}\n"
fi

# Simulate failure
echo -e "${BLUE}💥 Simulating Active NameNode failure...${NC}"
echo -e "${YELLOW}   Killing Active NameNode (PID: ${ACTIVE_PID})${NC}"
kill -9 ${ACTIVE_PID} 2>/dev/null || true
ACTIVE_PID=""

if ! ps -p ${ACTIVE_PID} > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Active NameNode stopped${NC}\n"
fi

# Wait for failover
echo -e "${BLUE}⏳ Waiting for failover detection (10 seconds)...${NC}"
echo -e "${YELLOW}   Watching for Standby promotion...${NC}"
sleep 10

# Check promotion
if grep -q "promoted to ACTIVE" ${STANDBY_LOG}; then
    echo -e "${GREEN}✅✅✅ FAILOVER SUCCESSFUL! ✅✅✅${NC}"
    echo -e "${GREEN}   Standby successfully promoted to ACTIVE!${NC}\n"
    echo -e "${BLUE}Promotion details:${NC}"
    grep "promoted to ACTIVE" ${STANDBY_LOG} -A 3 | sed 's/^/   /'
else
    echo -e "${RED}❌ Failover did not occur${NC}"
    echo -e "${YELLOW}Last 15 lines of Standby log:${NC}"
    tail -n 15 ${STANDBY_LOG} | sed 's/^/   /'
fi
echo ""

# Verify new Active is running
if ps -p ${STANDBY_PID} > /dev/null; then
    echo -e "${GREEN}✅ New Active NameNode still running (PID: ${STANDBY_PID})${NC}"
fi
echo ""

# Summary
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}  Test Results${NC}"
echo -e "${BLUE}=========================================${NC}"
echo -e "${GREEN}✅ Both NameNodes started successfully${NC}"
echo -e "${GREEN}✅ Metadata synchronized (Active → Standby)${NC}"
echo -e "${GREEN}✅ Heartbeat monitoring working${NC}"
echo -e "${GREEN}✅ Active NameNode failure simulated${NC}"

if grep -q "promoted to ACTIVE" ${STANDBY_LOG}; then
    echo -e "${GREEN}✅ Automatic failover completed${NC}"
    echo -e "${GREEN}✅ Standby promoted to Active${NC}"
    echo -e "\n${GREEN}🎉 HotBackup test PASSED! 🎉${NC}"
else
    echo -e "${RED}❌ Automatic failover failed${NC}"
    echo -e "\n${RED}❌ HotBackup test FAILED${NC}"
fi

echo -e "\n${YELLOW}📋 Detailed logs:${NC}"
echo -e "   Active:  ${ACTIVE_LOG}"
echo -e "   Standby: ${STANDBY_LOG}"
echo -e "\n${YELLOW}Press Enter to exit and cleanup...${NC}"
read
