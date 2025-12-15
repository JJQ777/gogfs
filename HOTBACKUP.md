# HotBackup (Standby NameNode) Feature

## Overview

The HotBackup feature implements a **Standby NameNode** that maintains a synchronized copy of the filesystem's metadata and is ready to take over instantly if the Active NameNode fails.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GoDFS with HotBackup                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌────────────────┐         ┌────────────────┐             │
│  │ Active NameNode│◄───────►│Standby NameNode│             │
│  │   (Port 8080)  │  Sync   │   (Port 8081)  │             │
│  └────────┬───────┘         └───────┬────────┘             │
│           │                         │                       │
│           │      Heartbeat          │                       │
│           │◄────────────────────────┘                       │
│           │                                                 │
│           │    Client Requests                              │
│           │◄────────────────                               │
│           │                                                 │
│           ▼                                                 │
│  ┌─────────────────────────────────┐                       │
│  │         DataNodes                │                       │
│  │  (Port 8001-8010)                │                       │
│  └─────────────────────────────────┘                       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Features

### 1. **Active NameNode**
- Handles all client requests (read/write)
- Manages file system metadata
- Automatically syncs metadata to Standby every 5 seconds
- Responds to heartbeat requests from Standby

### 2. **Standby NameNode**
- Maintains a synchronized copy of metadata
- Monitors Active NameNode health (every 2 seconds)
- Automatically promotes to Active if Active fails
- Persists metadata to disk for crash recovery

### 3. **Automatic Failover**
- Standby detects Active failure after 3 consecutive heartbeat failures (6 seconds)
- Automatic promotion to Active role
- No manual intervention required

### 4. **Metadata Synchronization**
- Real-time sync via gRPC
- Includes:
  - File-to-Block mappings
  - DataNode-to-Block mappings
  - DataNode metadata (ID, port, status)

## Usage

### Starting Active NameNode

```bash
make run-namenode
# Or manually:
./go-dfs namenode -port 8080 -block-size 32 -role active -peer localhost:8081
```

### Starting Standby NameNode

```bash
make run-namenode-standby
# Or manually:
./go-dfs namenode -port 8081 -block-size 32 -role standby -peer localhost:8080
```

### Testing Failover

Run the automated test script:

```bash
bash scripts/test_hotbackup.sh
```

This script will:
1. Start both Active and Standby NameNodes
2. Start DataNodes
3. Simulate Active NameNode failure
4. Demonstrate automatic failover

## Command Line Parameters

```
-port <port>        : Port number for NameNode (default: 8080)
-block-size <size>  : Block size in KB (default: 32)
-role <role>        : NameNode role - "active" or "standby" (default: "active")
-peer <address>     : Peer NameNode address for sync/monitor (e.g., "localhost:8081")
```

## Implementation Details

### RPC Methods for HotBackup

#### 1. SyncMetadata
```protobuf
rpc SyncMetadata (metadata) returns (status);
```
Active NameNode sends complete metadata snapshot to Standby.

#### 2. Heartbeat
```protobuf
rpc Heartbeat (namenodeInfo) returns (namenodeInfo);
```
Standby sends heartbeat to Active to check health status.

#### 3. PromoteToActive
```protobuf
rpc PromoteToActive (google.protobuf.Empty) returns (status);
```
Manually promote Standby to Active (for testing/maintenance).

### Failover Detection

```go
consecutiveFailures := 0
maxFailures := 3

// Monitor every 2 seconds
ticker := time.NewTicker(2 * time.Second)

if consecutiveFailures >= maxFailures {
    // Promote to Active
    nameNode.promoteToActive()
}
```

## Monitoring

### Active NameNode Logs
```
🔷 NameNode [a1b2c3d4] is listening on port :8080 as ACTIVE
✅ Connected to Standby NameNode at localhost:8081
📤 Synced metadata to Standby (Files: 10, DataNodes: 10)
```

### Standby NameNode Logs
```
🔷 NameNode [e5f6g7h8] is listening on port :8081 as STANDBY
🔄 Standby NameNode monitoring Active at localhost:8080
📥 Synced metadata from Active (Files: 10, DataNodes: 10)
```

### Failover Logs
```
⚠️  Failed to connect to Active NameNode (1/3): connection refused
⚠️  Heartbeat failed (2/3): connection refused
🚨 Active NameNode is DOWN! Promoting to Active...
======================================================================
✅ Successfully promoted to ACTIVE NameNode [e5f6g7h8]
📋 Current metadata state:
   - Files: 10
   - DataNodes: 10
   - Total Blocks: 250
======================================================================
```

## Configuration

### Sync Interval
Metadata sync occurs every **5 seconds** by default. Can be modified in:
```go
// namenode/namenode.go
ticker := time.NewTicker(5 * time.Second)
```

### Heartbeat Interval
Health check occurs every **2 seconds**. Can be modified in:
```go
// namenode/namenode.go
ticker := time.NewTicker(2 * time.Second)
```

### Failover Threshold
Failover triggers after **3 consecutive failures**. Can be modified in:
```go
// namenode/namenode.go
maxFailures := 3
```

## Benefits

1. **High Availability**: System continues operating even if Active NameNode fails
2. **Zero Data Loss**: Standby maintains synchronized metadata
3. **Fast Failover**: Automatic promotion within 6 seconds
4. **Persistent State**: Both nodes persist metadata to disk
5. **Simple Operation**: No manual intervention required

## Limitations & Future Improvements

### Current Limitations
- Only supports one Standby NameNode
- Manual recovery required after failover (restart failed Active)
- Clients must reconnect to new Active after failover

### Future Improvements
- [ ] Multiple Standby NameNodes
- [ ] Automatic client redirection
- [ ] Leader election using consensus algorithm (Raft/Paxos)
- [ ] Split-brain prevention
- [ ] Automatic Active NameNode recovery and re-sync

## Testing

### Unit Tests
```bash
go test ./namenode -v -run TestHotBackup
```

### Integration Test
```bash
bash scripts/test_hotbackup.sh
```

### Manual Test
1. Start Active NameNode
2. Start Standby NameNode
3. Perform some file operations
4. Kill Active NameNode process
5. Verify Standby promotes to Active
6. Verify system continues to operate

## Troubleshooting

### Standby not syncing
- Check network connectivity between nodes
- Verify peer address is correct
- Check firewall settings

### Failover not triggering
- Verify Active is truly down (check process)
- Check heartbeat interval and threshold
- Review Standby logs for error messages

### Both nodes claim to be Active
- This is a split-brain scenario
- Stop both nodes
- Manually designate one as Active, one as Standby
- Restart in correct order (Active first, then Standby)

## References

- HDFS High Availability: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html
- Raft Consensus: https://raft.github.io/
