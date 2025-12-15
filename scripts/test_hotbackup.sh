#!/bin/bash

# Test script for HotBackup NameNode feature
# This script demonstrates the Active/Standby NameNode failover

echo "========================================="
echo "HotBackup NameNode Test Script"
echo "========================================="
echo ""

# Build the project
echo "📦 Building project..."
make build

# Start Active NameNode
echo "🔷 Starting Active NameNode on port 8080..."
./go-dfs namenode -port 8080 -block-size 32 -role active -peer localhost:8081 &
ACTIVE_PID=$!
sleep 2

# Start Standby NameNode
echo "🔷 Starting Standby NameNode on port 8081..."
./go-dfs namenode -port 8081 -block-size 32 -role standby -peer localhost:8080 &
STANDBY_PID=$!
sleep 2

echo ""
echo "✅ Both NameNodes are running:"
echo "   Active NameNode:  PID=$ACTIVE_PID (port 8080)"
echo "   Standby NameNode: PID=$STANDBY_PID (port 8081)"
echo ""

# Start some datanodes
echo "🔷 Starting DataNodes..."
bash scripts/run_datanodes.sh &
DATANODE_SCRIPT_PID=$!
sleep 3

echo ""
echo "📊 System Status:"
echo "   - Active NameNode syncing to Standby"
echo "   - Standby NameNode monitoring Active"
echo "   - DataNodes connected"
echo ""

# Wait for user input to test failover
echo "Press Enter to simulate Active NameNode failure..."
read

# Kill Active NameNode to simulate failure
echo "🚨 Simulating Active NameNode failure..."
kill $ACTIVE_PID
echo "   Killed Active NameNode (PID=$ACTIVE_PID)"
echo ""

# Wait for failover
echo "⏳ Waiting for Standby to detect failure and promote itself..."
sleep 8

echo ""
echo "========================================="
echo "✅ Failover Test Complete!"
echo "========================================="
echo ""
echo "The Standby NameNode should have detected the failure"
echo "and promoted itself to Active."
echo ""
echo "Press Enter to cleanup and exit..."
read

# Cleanup
echo "🧹 Cleaning up..."
kill $STANDBY_PID 2>/dev/null
pkill -P $DATANODE_SCRIPT_PID 2>/dev/null
pkill -f "go-dfs datanode" 2>/dev/null

echo "✅ Cleanup complete!"
