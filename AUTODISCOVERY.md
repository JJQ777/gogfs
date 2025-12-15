# Auto-Discovery Architecture (自动发现架构)

## 📋 概述

新架构解决了原有 HotBackup 实现中的关键问题：**DataNode 和 Client 在故障转移后无法自动连接到新的 Active NameNode**。

## 🔧 架构改进

### 问题分析
原架构中：
- DataNode 硬编码连接到端口 8080
- Client 需要手动指定 NameNode 端口
- 故障转移后，无法自动发现新的 Active NameNode
- DataNode 无法自动重连到新 Active

### 解决方案
实现了 **NameNode 集群配置和自动发现机制**：

1. **集群配置文件** (`namenode_cluster.conf`)
   - 定义所有 NameNode 地址
   - 指定 Primary NameNode
   - 配置故障转移参数

2. **自动发现服务** (`utils/namenode_discovery.go`)
   - `LoadClusterConfig()`: 加载集群配置
   - `DiscoverActiveNamenode()`: 自动发现可用的 Active NameNode
   - `tryConnect()`: 带超时的连接尝试

3. **智能连接逻辑**
   - DataNode 启动时自动发现 Active NameNode
   - Client 使用配置文件自动连接
   - 故障转移后自动切换到新 Active

## 📁 新增文件

### 1. `namenode_cluster.conf`
```
# NameNode 集群配置
namenodes=localhost:8080,localhost:8081
primary=localhost:8080
failover_timeout=5
retry_interval=2
```

### 2. `utils/namenode_discovery.go`
提供集群发现功能的核心组件。

### 3. `scripts/test_autodiscovery.sh`
测试自动发现和故障转移的完整脚本。

## 🚀 使用方法

### 启动 NameNode 集群

**Active NameNode:**
```bash
./go-dfs namenode -port 8080 -role active -peer localhost:8081 -block-size 32
```

**Standby NameNode:**
```bash
./go-dfs namenode -port 8081 -role standby -peer localhost:8080 -block-size 32
```

### 启动 DataNode（自动发现）

```bash
# DataNode 会自动从配置文件读取 NameNode 集群信息并连接
./go-dfs datanode -port 8001 -location datanode-files -config namenode_cluster.conf
```

如果不指定 `-config`，会使用默认的 `namenode_cluster.conf`。

### 客户端操作（自动发现）

**写入文件:**
```bash
./go-dfs client -operation write -filename test.txt -source-path . -config namenode_cluster.conf
```

**读取文件:**
```bash
./go-dfs client -operation read -filename test.txt -source-path . -config namenode_cluster.conf
```

客户端会自动：
1. 尝试连接 Primary NameNode (localhost:8080)
2. 如果失败，尝试其他 NameNode (localhost:8081)
3. 连接到第一个可用的 Active NameNode

## 🔄 故障转移流程

### 自动故障转移步骤：

1. **Active NameNode 故障**
   - Active NameNode (8080) 停止服务
   
2. **Standby 检测故障**
   - Standby (8081) 通过心跳检测到 Active 失败
   - 连续 3 次心跳失败（约 6 秒）
   
3. **自动升级**
   - Standby 自动升级为 Active
   - 日志显示: `✅ Successfully promoted to ACTIVE NameNode`
   
4. **Client 自动重连**
   - Client 下次操作时自动发现新 Active
   - 无需手动修改配置或端口
   
5. **DataNode 自动重连**
   - DataNode 启动时就配置了所有 NameNode 地址
   - 可以连接到任何可用的 Active NameNode

## 🧪 完整测试

运行自动发现测试脚本：

```bash
bash scripts/test_autodiscovery.sh
```

### 测试内容：

1. ✅ 启动 Active 和 Standby NameNode
2. ✅ 验证元数据同步
3. ✅ 启动 DataNode（使用自动发现）
4. ✅ 写入文件（Client 使用自动发现）
5. ✅ 读取文件验证
6. ✅ 模拟 Active 故障
7. ✅ 验证自动故障转移
8. ✅ Client 自动重连到新 Active
9. ✅ 读取文件验证（通过新 Active）
10. ✅ 写入新文件验证

### 预期结果：

```
✅ Active and Standby NameNodes started
✅ Metadata synchronized
✅ DataNodes connected with auto-discovery
✅ File operations successful before failover
✅ Active NameNode failure simulated
✅ Standby promoted to Active automatically
✅ Auto-discovery reconnected to new Active
✅ File operations successful after failover

🎉 HotBackup with Auto-Discovery test PASSED! 🎉
```

## 📊 架构对比

### 旧架构 (硬编码端口)
```
Client → NameNode:8080 (固定)
DataNode → NameNode:8080 (固定)

故障转移后：
Client → ❌ 无法连接
DataNode → ❌ 无法连接
```

### 新架构 (自动发现)
```
Client → namenode_cluster.conf → 自动发现 Active
DataNode → namenode_cluster.conf → 自动发现 Active

故障转移后：
Client → ✅ 自动连接到新 Active (8081)
DataNode → ✅ 启动时已配置所有地址
```

## 🔍 关键代码

### 自动发现流程

```go
// 加载集群配置
config, err := utils.LoadClusterConfig("namenode_cluster.conf")

// 发现可用的 Active NameNode
conn, activeAddr, err := utils.DiscoverActiveNamenode(config)

// 连接成功，activeAddr 是当前 Active 的地址
log.Printf("Connected to Active NameNode at %s", activeAddr)
```

### 连接重试逻辑

1. 尝试连接 Primary (localhost:8080)
2. 如果失败，遍历所有 NameNode
3. 返回第一个成功连接的 NameNode
4. 带 3 秒超时机制

## 🎯 优势

1. **高可用性**: 自动故障转移，无需人工介入
2. **透明重连**: Client 和 DataNode 无感知切换
3. **易于配置**: 单一配置文件管理集群
4. **容错能力**: 多个 NameNode 地址，自动选择可用节点
5. **可扩展性**: 轻松添加更多 Standby NameNode

## 🚧 后续优化建议

1. **健康检查**: DataNode 定期重新发现 Active（如果连接断开）
2. **负载均衡**: 读操作可以分散到多个 Standby
3. **ZooKeeper 集成**: 使用 ZooKeeper 进行分布式协调
4. **心跳优化**: 自适应心跳间隔
5. **优雅降级**: Active 主动通知 Client/DataNode 切换

## 📝 配置文件说明

### `namenode_cluster.conf` 参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `namenodes` | NameNode 地址列表（逗号分隔） | `localhost:8080,localhost:8081` |
| `primary` | 首选的 Primary NameNode | `localhost:8080` |
| `failover_timeout` | 故障转移超时（秒） | `5` |
| `retry_interval` | 重试间隔（秒） | `2` |

## 🔐 安全考虑

- 当前使用 `insecure.NewCredentials()` 进行连接
- 生产环境建议使用 TLS 加密
- 添加认证机制验证 NameNode 身份

---

**版本**: 2.0 with Auto-Discovery  
**作者**: GitHub Copilot  
**日期**: 2025-12-03
