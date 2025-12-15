# 完整高可用架构实现

## 🎯 方案2：DataNode 自动重连机制（最全面）

### 核心功能

1. **自动发现** - DataNode 和 Client 通过配置文件自动发现 Active NameNode
2. **心跳监控** - DataNode 每10秒检查与 NameNode 的连接状态
3. **自动重连** - 检测到连接断开后，自动重新发现并连接新的 Active NameNode
4. **自动注册** - 重连后自动向新 NameNode 注册
5. **故障转移** - Standby NameNode 检测到 Active 故障后自动升级

### 架构组件

#### 1. NameNode 集群配置 (`namenode_cluster.conf`)
```
namenodes=localhost:8080,localhost:8081
primary=localhost:8080
```

#### 2. 自动发现服务 (`utils/namenode_discovery.go`)
- `LoadClusterConfig()` - 加载集群配置
- `DiscoverActiveNamenode()` - 查找可用的 Active NameNode
- `tryConnect()` - 带超时的连接尝试

#### 3. DataNode 自动重连 (`datanode/datanode.go`)

**新增字段**:
```go
type DataNode struct {
    ID               string
    Port             string      // DataNode 端口
    ConfigPath       string      // 配置文件路径
    ActiveConn       *grpc.ClientConn // 当前活动连接
    ...
}
```

**新增方法**:
- `ConnectToActiveNameNode()` - 使用自动发现连接
- `MonitorConnectionAndReconnect()` - 监控连接并自动重连

**重连流程**:
```
1. 每10秒发送心跳到 NameNode
2. 如果心跳失败（连接断开）
   ├─ 关闭旧连接
   ├─ 从配置文件重新发现 Active NameNode
   ├─ 建立新连接
   ├─ 重新注册到新 NameNode
   └─ 重新启动块报告
3. 继续监控
```

#### 4. Client 自动发现 (`client/client.go`)
- 智能地址格式处理
- 支持 `host:port` 格式直接使用
- 空 DataNode 列表安全检查

### 工作流程

#### 正常运行
```
┌─────────────┐     sync      ┌─────────────┐
│   Active    │◄─────────────►│   Standby   │
│ NameNode    │   heartbeat   │  NameNode   │
│  (8080)     │               │   (8081)    │
└──────┬──────┘               └─────────────┘
       │
       │ register + heartbeat
       │
   ┌───┴───┬───────┬───────┐
   │       │       │       │
┌──▼──┐ ┌──▼──┐ ┌──▼──┐ ┌──▼──┐
│DN 1 │ │DN 2 │ │DN 3 │ │DN 4 │
└─────┘ └─────┘ └─────┘ └─────┘
```

#### 故障转移流程
```
1. Active NameNode (8080) 故障
   ↓
2. Standby (8081) 检测到3次心跳失败 (6秒)
   ↓
3. Standby 自动升级为 Active
   ↓
4. DataNodes 检测到连接断开 (最多10秒)
   ↓
5. DataNodes 重新发现 Active (现在是8081)
   ↓
6. DataNodes 重连到新 Active 并重新注册
   ↓
7. 系统恢复正常，Client 可以正常读写文件
```

#### 时间线
```
T=0s    : Active (8080) 运行，Standby (8081) 监控
T=0s    : DataNodes 连接到 Active (8080)
T=0s    : Active 故障（被杀死）
T=2s    : Standby 第1次心跳失败
T=4s    : Standby 第2次心跳失败  
T=6s    : Standby 第3次心跳失败 → 升级为 Active
T=10s   : DataNode 心跳检测到连接断开
T=12s   : DataNode 重新发现 Active (8081)
T=13s   : DataNode 重连成功并注册
T=15s   : 系统完全恢复
```

### 关键参数

| 参数 | 值 | 说明 |
|------|-----|------|
| Standby 心跳间隔 | 2秒 | 检测 Active 是否存活 |
| Standby 故障阈值 | 3次失败 | 触发升级为 Active |
| 故障转移时间 | ~6秒 | 从 Active 故障到 Standby 升级 |
| DataNode 监控间隔 | 10秒 | 检查连接状态 |
| 连接超时 | 3秒 | 单次连接尝试超时 |
| 完全恢复时间 | ~15秒 | 从故障到完全恢复 |

### 测试验证

运行完整测试：
```bash
bash scripts/test_autodiscovery.sh
```

测试步骤：
1. ✅ 启动 Active 和 Standby NameNode
2. ✅ 验证元数据同步 (每5秒)
3. ✅ 启动 DataNodes (自动发现 + 自动重连)
4. ✅ 写入文件到 Active
5. ✅ 从 Active 读取文件验证
6. ✅ 模拟 Active 故障
7. ✅ 验证 Standby 自动升级
8. ✅ 验证 DataNodes 自动重连
9. ✅ 从新 Active 读取文件
10. ✅ 写入新文件验证系统完全恢复

### 优势对比

| 特性 | 方案1 (简化测试) | 方案2 (自动重连) | 方案3 (手动重启) |
|------|-----------------|-----------------|-----------------|
| 实现难度 | ⭐ 简单 | ⭐⭐⭐ 复杂 | ⭐⭐ 中等 |
| 测试覆盖 | 仅故障转移 | 完整流程 | 完整流程 |
| 生产可用 | ❌ 不可用 | ✅ 可用 | ❌ 需要手动干预 |
| 自动化程度 | 低 | 高 | 中 |
| 恢复时间 | N/A | ~15秒 | 取决于手动速度 |
| 数据可用性 | ❌ 故障后不可读写 | ✅ 自动恢复 | ⚠️ 需要手动操作 |

### 实现的文件

1. **datanode/datanode.go**
   - 添加 `Port`, `ConfigPath`, `ActiveConn` 字段
   - 实现 `ConnectToActiveNameNode()` 方法
   - 实现 `MonitorConnectionAndReconnect()` 方法

2. **main.go**
   - 更新 DataNode 启动逻辑
   - 启动自动重连监控

3. **client/client.go**
   - 智能地址格式处理
   - 空列表安全检查

4. **scripts/test_autodiscovery.sh**
   - 更新测试流程
   - 添加重连验证

### 生产环境建议

1. **调整心跳间隔**: 根据网络环境调整检测间隔
2. **添加日志**: 更详细的重连日志便于排查
3. **健康检查**: 添加 HTTP 健康检查接口
4. **监控告警**: 集成 Prometheus/Grafana 监控
5. **优雅关闭**: 实现优雅关闭，保存状态
6. **限流重试**: 避免大量 DataNode 同时重连导致雪崩

### 未来优化

1. **指数退避重试**: 连接失败时使用指数退避
2. **连接池**: 复用 gRPC 连接
3. **分布式协调**: 使用 etcd/ZooKeeper
4. **多 Standby**: 支持多个 Standby NameNode
5. **负载均衡**: 读操作分散到多个节点
6. **数据迁移**: Active 切换时的数据重平衡

---

**总结**: 方案2是最全面的实现，提供了真正的生产级高可用性，DataNode 可以在 Active 故障后自动恢复，无需人工干预。
