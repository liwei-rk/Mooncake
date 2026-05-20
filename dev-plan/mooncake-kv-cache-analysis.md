# Mooncake KV Cache 存储实现分析

> 基于对 `mooncake-store/src/` 和 `mooncake-store/include/` 的完整代码分析，记录当前 Mooncake 中 KV cache 的存储架构、调用路径、以及 GDS 集成现状。

---

## 一、整体架构

Mooncake 的 KV cache 存储采用 **分层架构**，由 Master-Slave 模式管理分布式节点上的内存和磁盘资源：

```
┌──────────────────────────────────────────────────────────────┐
│                    Client (client_service.h)                 │
│  Put / Get / Query / MountSegment / OffloadObjectHeartbeat │
└────────────┬──────────────────────────┬──────────────────────┘
             │                          │
             ▼                          ▼
┌─────────────────────────┐  ┌─────────────────────────────────┐
│      MasterService      │  │      TransferSubmitter          │
│  (master_service.h)     │  │  (transfer_task.h)              │
│  - 元数据管理(1024分片)  │  │  - 策略选择: LOCAL_MEMCPY /     │
│  - 副本分配/回收         │  │    TRANSFER_ENGINE / FILE_READ │
│  - 租约/淘汰             │  │  - MemcpyWorkerPool            │
│  - Segment管理           │  │  - FilereadWorkerPool          │
└────────────┬─────────────┘  └──────────────┬──────────────────┘
             │                               │
             ▼                               ▼
┌──────────────────────────────────────────────────────────────┐
│               StorageBackendInterface (storage_backend.h)    │
│  BatchOffload / BatchLoad / IsExist / ScanMeta              │
└──────┬────────────────┬──────────────────┬───────────────────┘
       │                │                  │
       ▼                ▼                  ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────────────────┐
│ StorageBackend│ │BucketStorage │ │OffsetAllocatorStorage    │
│ (FilePerKey) │ │   Backend    │ │        Backend           │
│ → NDS::put() │ │ 分桶存储     │ │ 偏移分配器存储           │
└──────────────┘ └──────────────┘ └──────────────────────────┘
```

---

## 二、核心组件详解

### 1. Segment 与内存管理（segment.h / allocator.h）

每个节点通过 `MountSegment` 向 Master 注册一段内存区域（默认 16MB），Master 的 `SegmentManager` 管理所有已挂载的 Segment：

- **CachelibBufferAllocator** (allocator.h:140)：基于 Facebook CacheLib 的 Slab 分配器，适合大量小对象
- **OffsetBufferAllocator** (allocator.h:191)：基于偏移量的分配器，能精确追踪最大空闲区域

每个 `Segment` 包含：唯一 ID、名称（通常为 `ip:port`）、基地址、大小、传输端点（用于 RDMA/TCP）。

### 2. Master 元数据管理（master_service.h）

Master 是控制面核心，采用 **1024 分片**的无锁化设计：

- **MetadataShard**：每个分片有独立的 `SharedMutex`，存储 `key → ObjectMetadata` 映射
- **ObjectMetadata** (master_service.h:472)：记录 client_id、大小、租约超时、软 Pin 超时、Replica 列表
- **Replica** (replica.h:146)：三种类型 — `MEMORY`（内存副本）、`DISK`（磁盘副本）、`LOCAL_DISK`（本地磁盘副本）

### 3. Replica 与数据定位（replica.h）

每个 KV 对象可以有多个 Replica（支持多副本），数据载体类型：

| 类型 | 数据结构 | 用途 |
|------|---------|------|
| **MEMORY** | MemoryReplicaData (含 AllocatedBuffer) | RDMA/TCP 直接访问的内存数据 |
| **DISK** | DiskReplicaData (含 file_path, object_size) | 持久化到共享存储的文件 |
| **LOCAL_DISK** | LocalDiskReplicaData (含 client_id, transport_endpoint) | 持久化到本地磁盘的数据 |

### 4. 存储后端层次（storage_backend.h）

`StorageBackendInterface` 定义了统一接口，三种实现：

- **StorageBackendAdaptor** (storage_backend.h:638)：FilePerKey 模式，当前 StoreObject 已改为 NDS::put()
- **BucketStorageBackend** (storage_backend.h:705)：分桶策略，多对象聚合写入一个大文件，支持 FIFO/LRU 淘汰
- **OffsetAllocatorStorageBackend** (storage_backend.h:1006)：固定大文件 + 偏移分配器，1024 分片元数据

---

## 三、KV Cache 可存储的介质

### 1. 内存 DRAM（ReplicaType::MEMORY）
- **始终存在**，每次 PutStart 都通过 AllocationStrategy 从 Segment 分配
- 分配策略：RANDOM / FREE_RATIO_FIRST / CXL
- 传输：本地 memcpy，远程 TransferEngine (RDMA/TCP)

### 2. 磁盘文件（ReplicaType::DISK）
- 触发条件：MasterConfig 中 `root_fs_dir` 非空 → `use_disk_replica_ = true`
- 写路径：PutToLocalFile → StorageBackend::StoreObject → NDS::put()（已替换原文件写）
- 读路径：仍通过 LoadObject 读本地文件（**NDS::get() 尚未集成**）
- 淘汰：FIFO / LRU

### 3. 本地磁盘 Offload（ReplicaType::LOCAL_DISK）
- 触发条件：`enable_offload = true`
- Client 通过 MountLocalDiskSegment 注册本地磁盘
- PutEnd 时推入 offloading_tasks，通过 FileStorage::OffloadObjects 写入
- 支持 BucketStorageBackend / OffsetAllocatorStorageBackend

### 4. NDS / GDS KV 引擎
- 接口定义：nds_interface.h（init / put / get / isExists）
- key 通过 objectKeyToUint64() 转换为 uint64_t blockId
- 当前仅 StorageBackend::StoreObject 调用了 NDS::put()，get 未集成

### 5. CXL 共享内存
- 触发条件：`enable_cxl = true`, protocol = "cxl"
- 基于 /dev/dax0.0 的大页内存，默认基地址 0x100000000，默认大小 8GB
- CxlAllocationStrategy 覆盖默认分配策略

### 6. 本地热缓存（Client 端）
- 环境变量 `MC_STORE_LOCAL_HOT_CACHE_SIZE` 配置
- CountMinSketch 频率准入（默认阈值 2）
- 仅加速 MEMORY 副本的读取

---

## 四、决定 KV cache 存储在何种介质中的逻辑

一个 key 可以同时拥有多个不同类型的 Replica。决定因素：

| 配置 | MEMORY | DISK | LOCAL_DISK | CXL | Hot Cache |
|------|:------:|:----:|:----------:|:---:|:---------:|
| 默认 | 有 | 无 | 无 | 无 | 取决于环境变量 |
| `root_fs_dir` 非空 | 有 | **有** | 无 | 无 | 同上 |
| `enable_offload=true` | 有 | 无 | **有** | 无 | 同上 |
| `enable_cxl=true` | 有 | 无 | 无 | **有** | 同上 |
| 全开 | 有 | 有 | 有 | 有 | 同上 |

关键代码路径：

```
MasterService::PutStart()                          // master_service.cpp:664
  │
  ├─ allocation_strategy_->Allocate() → MEMORY     // :732 始终执行
  ├─ if (use_disk_replica_) → DISK                  // :750 root_fs_dir 非空
  └─ if (enable_cxl_) → CXL 策略覆盖               // master_service.cpp:147
```

Client 端处理：

```
Client::Put() / Client::BatchPut()
  │
  ├─ DISK Replica → PutToLocalFile
  │    └─ StorageBackend::StoreObject → NDS::put()   // storage_backend.cpp:310
  │
  └─ MEMORY Replica → TransferSubmitter::submit()
       ├─ 本地 → LOCAL_MEMCPY
       └─ 远程 → TRANSFER_ENGINE
```

---

## 五、数据写入流程

### 单 key Put（client_service.cpp:1050）

```
Client::Put(key, slices, config)
  ├─ 1. MasterClient::PutStart() → Master 分配副本
  ├─ 2. DISK 副本 → PutToLocalFile → NDS::put() (异步线程池)
  ├─ 3. MEMORY 副本 → TransferWrite() → TransferSubmitter::submit()
  └─ 4. MasterClient::PutEnd() → 标记完成，设置租约
```

### 批量 BatchPut（client_service.cpp:1615）

```
Client::BatchPut(keys, batched_slices, config)
  ├─ 1. CreatePutOperations()          // 封装为 PutOperation 对象
  ├─ 2. StartBatchPut()                // 1 次 RPC 批量分配副本
  │      └─ MasterClient::BatchPutStart() → 循环 N 次 PutStart
  ├─ 3. SubmitTransfers()              // 提交所有传输
  │      ├─ DISK → PutToLocalFile (每个 key 串行)
  │      └─ MEMORY → transfer_submitter_->submit() → TransferFuture
  ├─ 4. WaitForTransfers()             // 统一等待所有 future 完成
  ├─ 5. FinalizeBatchPut()             // 成功 key → BatchPutEnd, 失败 key → BatchPutRevoke
  └─ 6. CollectResults()              // 返回 per-key 结果
```

单 key vs Batch 核心区别：

| 阶段 | 单 key | 批量 |
|------|--------|------|
| 副本分配 | PutStart (1 RPC) | BatchPutStart (1 RPC 循环 N 次) |
| 磁盘写入 | 同步 PutToLocalFile | 同步 PutToLocalFile（内部异步线程池） |
| 内存传输 | 同步 TransferWrite | 异步 submit → Future，全部提交后再统一 wait |
| 完成通知 | PutEnd (1 RPC) | BatchPutEnd / BatchPutRevoke (1-2 RPC) |

---

## 六、数据读取流程

```
Client::Get(key, slices)
  ├─ 1. Query() → MasterClient::GetReplicaList() → 返回副本列表 + 租约
  │
  └─ 2. TransferRead() → TransferSubmitter::submit()
        ├─ MEMORY → LOCAL_MEMCPY 或 TRANSFER_ENGINE
        │   └─ 可选：RedirectToHotCache() 热缓存命中
        └─ DISK → submitFileReadOperation()
              → FilereadWorkerPool (异步线程池)
              → StorageBackend::LoadObject() (文件 vector_read)
```

---

## 七、GDS KV 引擎集成现状

**不是完全替代，而是部分替换**：

| 组件 | 写入 | 读取 | 状态 |
|------|:----:|:----:|:----:|
| StorageBackend (FilePerKey) | NDS::put() | 仍读文件 | **只替换了写** |
| BucketStorageBackend | 本地文件 | 本地文件 | 未集成 |
| OffsetAllocatorStorageBackend | 本地文件 | 本地文件 | 未集成 |

证据：
- 整个 storage_backend.cpp 只有一处 `NDS::put` 调用（第 310 行）
- 没有 `NDS::get` 调用
- 旧的文件写入代码被 `#if 0 ... #endif` 保留（未删除）
- 这是一个**渐进式迁移**过程

---

## 八、新增 GDS 存储模式的架构方案

当前做法（直接修改 StoreObject）是把 GDS **伪装成 DISK**，无法与原有 DISK 共存。

正确做法是**新增一种独立的 ReplicaType**，需要改动 5 层：

### 1. ReplicaType 新增枚举（replica.h:29）

```cpp
enum class ReplicaType {
    MEMORY,
    DISK,
    LOCAL_DISK,
    GDS,        // [新增]
};
```

### 2. Replica 新增数据结构（replica.h:113-126）

```cpp
struct GdsReplicaData {
    uint64_t block_id;
    uint64_t object_size;
};
struct GdsDescriptor {
    uint64_t block_id;
    uint64_t object_size;
    YLT_REFL(GdsDescriptor, block_id, object_size);
};
```

Replica 的 variant 和 Descriptor 的 variant 都要增加对应类型。

### 3. MasterConfig / MasterServiceConfig 新增开关

```cpp
bool enable_gds = false;
```

### 4. MasterService 新增标识 + PutStart 分支

```cpp
// 构造函数：enable_gds_ = config.enable_gds;
// PutStart 中参照 use_disk_replica_ 模式：
if (enable_gds_) {
    uint64_t block_id = objectKeyToUint64(key);
    replicas.emplace_back(block_id, total_length, ReplicaStatus::PROCESSING);
}
```

### 5. Client::SubmitTransfers 新增判断分支

```cpp
// 参照 DISK 的处理方式，新增：
if (replica.is_gds_replica()) {
    auto gds_desc = replica.get_gds_descriptor();
    NDS::put(gds_desc.block_id, data, 0, gds_desc.object_size);
}
```

| 维度 | 当前做法（替换 StoreObject） | 正确做法（新增 ReplicaType） |
|------|---------------------------|---------------------------|
| 与 DISK 共存 | 不可能 | 可以共存 |
| 运行时开关 | 无 | `enable_gds` 配置控制 |
| Master 感知 | 不感知 | 独立统计管理 |
| 读取区分 | 无法区分 | 按 ReplicaType 调用不同接口 |
| 回退/灰度 | 改代码重编译 | 改配置即可 |

---

## 九、淘汰与空间管理

### 内存淘汰（Master 端）
- 高水位触发：used/capacity > `eviction_high_watermark_ratio`（默认 95%）
- 两级淘汰：1) 无 soft pin 对象 2) soft pin 对象（如允许）
- 租约：lease_timeout + 可选 soft_pin_timeout

### 磁盘淘汰（Client 端）
- StorageBackend：FIFO 文件队列 + EnsureDiskSpace()
- BucketStorageBackend：PrepareEviction + FinalizeEviction 两阶段，FIFO/LRU

---

## 十、高可用机制

- **Snapshot/Restore**：fork() 子进程序列化 msgpack，支持上传到备份目录
- **Leader 选举**：基于 etcd 的 Leader 选举 + Master 监控
- **etcd OpLog**：OpLogWatcher + OpLogApplier 重放

---

## 十一、测试文件

### 主集成测试：[client_integration_test.cpp](mooncake-store/tests/client_integration_test.cpp)

| 测试用例 | 行号 | 说明 |
|---------|:----:|------|
| BatchPutGetOperations | :572 | 100 个 key BatchPut + BatchGet + 性能对比 |
| BatchIsExistOperations | :661 | 50 个 key 部分存在性检查 |
| BatchPutDuplicateKeys | :833 | 重复 key 幂等行为 |
| BatchReplicaClearOperations | :884 | 批量清除 |

### GDS 专用测试：[client_gds_integration_test.cpp](mooncake-store/tests/client_gds_integration_test.cpp)

| 测试用例 | 行号 | 说明 |
|---------|:----:|------|
| BasicPutGetOperations | :50 | 单 key GDS 路径 |
| BatchPutGetOperations | :92 | 5 个 key BatchPut/BatchGet |
| ReadNonExistentKey | :162 | 不存在 key |
| EmptyData | :185 | 空数据 |

### 其他批测试文件
- pybind_client_test.cpp — Python 绑定层 batch_get_into / batch_put_from
- client_metrics_test.cpp — 批操作指标
- master_metrics_test.cpp — Master 侧 BatchPutStart/BatchPutEnd 指标
- dummy_client_get_buffer_test.cpp — Dummy 客户端批操作
- cxl_client_integration_test.cpp — CXL 路径批操作

---

## 十二、关键文件索引

| 文件 | 主要内容 |
|------|---------|
| include/types.h | ErrorCode、Slice、Segment、StorageObjectMetadata |
| include/allocator.h | BufferAllocatorBase、CachelibBufferAllocator、OffsetBufferAllocator |
| include/segment.h | SegmentManager、MountedSegment、ScopedSegmentAccess |
| include/replica.h | ReplicaType、ReplicaStatus、Replica、ReplicateConfig |
| include/master_service.h | MasterService、ObjectMetadata、MetadataShard |
| include/master_config.h | MasterConfig、WrappedMasterServiceConfig |
| include/storage_backend.h | StorageBackendInterface、StorageBackend、BucketStorageBackend、OffsetAllocatorStorageBackend |
| include/rpc_service.h | WrappedMasterService RPC 接口 |
| include/rpc_types.h | PingResponse、GetReplicaListResponse、TaskAssignment |
| include/client_service.h | Client、QueryResult、PutOperation |
| include/transfer_task.h | TransferSubmitter、TransferFuture、TransferStrategy |
| include/allocation_strategy.h | RandomAllocationStrategy、FreeRatioFirstAllocationStrategy、CxlAllocationStrategy |
| include/nds/nds_interface.h | NDS::init/put/get/isExists 接口定义 |
| src/storage_backend.cpp | 存储后端实现，NDS::put 集成 |
| src/master_service.cpp | PutStart/PutEnd/PutRevoke、淘汰/Offload 逻辑 |
| src/client_service.cpp | Client::Put/BatchPut/Get/BatchGet 实现 |
| src/rpc_service.cpp | RPC 分发层 |
| src/segment.cpp | Segment 挂载/卸载/持久化 |