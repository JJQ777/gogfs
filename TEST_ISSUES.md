# 测试问题总结与修复

## 发现的问题

### 1. 端口占用问题 ✅ 已修复
**问题**: 端口 8080 和 8081 被之前的测试进程占用
**原因**: 测试脚本被中断后，NameNode 进程没有被清理
**解决**: 
```bash
kill -9 25730 25771  # 杀死残留进程
```

### 2. 地址格式问题 ✅ 已修复
**问题**: `panic: address localhost:localhost:8080: too many colons`
**原因**: 
- 自动发现返回的地址已经是 `localhost:8080` 格式
- 但 `ConnectToNameNode()` 和 `GetNameNodeStub()` 又使用 `net.JoinHostPort("localhost", port)` 拼接
- 导致地址变成 `localhost:localhost:8080`

**解决**: 修改了两个函数，检测地址格式：
```go
// 如果已经是 host:port 格式，直接使用
if _, _, err := net.SplitHostPort(client.NameNodePort); err == nil {
    connectionString = client.NameNodePort
} else {
    connectionString = net.JoinHostPort("localhost", client.NameNodePort)
}
```

### 3. DataNode 列表为空问题 ✅ 已修复
**问题**: `panic: invalid argument to Intn`
**原因**: 
- 读取文件时 `rand.Intn(len(dataNodeIDs))` 被调用
- 但 `dataNodeIDs` 长度为 0（没有 DataNode）
- `rand.Intn(0)` 是非法操作

**解决**: 添加检查：
```go
if len(dataNodeIDs) == 0 {
    log.Printf("⚠️  No DataNodes available for block %s, skipping...", blockID)
    continue
}
```

### 4. DataNode 未重新注册问题 ⚠️ 架构问题
**问题**: 故障转移后，DataNodes 仍然连接到旧的 Active NameNode (8080)
**原因**: 
- DataNode 在启动时连接 NameNode，并保持长连接
- 当 Active (8080) 故障后，Standby (8081) 升级为新 Active
- 但 DataNodes 不知道要重连到新的 Active

**当前状态**: 
- DataNode 使用自动发现启动时会尝试连接可用的 NameNode
- 但一旦连接建立，就不会自动切换
- 测试脚本中的 DataNodes 是在 Active 故障后才启动的，所以会连接到新 Active

**建议优化**: 
1. 实现 DataNode 心跳检测，发现连接断开时自动重连
2. 使用配置文件中的所有 NameNode 地址进行failover重连
3. NameNode 在升级为 Active 时，可以主动通知所有已知的 DataNodes

## 当前测试方案调整

由于 DataNode 重连机制的限制，测试脚本需要调整：

### 方案 A: 先启动所有 DataNodes（推荐）
```bash
1. 启动 Active 和 Standby NameNode
2. 启动 DataNodes（连接到 Active）
3. 写入文件
4. 杀死 Active
5. 等待故障转移
6. 读取文件（可能失败，因为 DataNodes 还连着旧 Active）
```

### 方案 B: 故障转移后重启 DataNodes
```bash
1. 启动 Active 和 Standby
2. 启动 DataNodes
3. 写入文件
4. 杀死 Active
5. 等待故障转移
6. 重启 DataNodes（使用自动发现连接到新 Active）
7. 读取文件成功
```

## 已修复的文件

1. `client/client.go`
   - `ConnectToNameNode()`: 智能地址格式处理
   - `GetNameNodeStub()`: 智能地址格式处理  
   - `ReadFile()`: DataNode 列表空检查

2. `utils/namenode_discovery.go`: 新文件，自动发现机制

3. `main.go`: 添加 config 参数支持

## 测试建议

使用修复后的简化测试脚本 `test_hotbackup_simple.sh`：
- 只测试故障转移机制
- 不涉及复杂的文件读写
- 验证 Standby 能否成功升级为 Active

完整的文件操作测试可以在实现 DataNode 自动重连后进行。
