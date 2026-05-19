# Client::BatchGet 到 StorageBackend 的完整调用栈

## 概述

`Client::BatchGet` 用于批量获取对象数据。根据 replica 类型（内存副本或磁盘副本），调用路径不同。本文档主要描述**磁盘副本**场景下的调用栈。

## 磁盘副本调用栈

```
Client::BatchGet (mooncake-store/src/client_service.cpp:871)
    │
    ├─→ Client::BatchQuery (client_service.cpp:571)
    │       调用 MasterClient::BatchGetReplicaList 获取 replica 列表
    │
    ├─→ FindFirstCompleteReplica (client_service.cpp:928)
    │       从 replica 列表中找到第一个完整的 replica
    │
    ├─→ 判断 replica 类型 (is_memory_replica())
    │
    │   [如果是 disk replica]
    │
    └─→ TransferSubmitter::submit (mooncake-store/src/transfer_task.cpp:438)
            │
            │   判断 replica.is_memory_replica() == false (行 443-466)
            │   进入 submitFileReadOperation 分支
            │
            └─→ TransferSubmitter::submitFileReadOperation (transfer_task.cpp:664)
                    │
                    │   创建 FilereadTask (行 667-673)
                    │   包含: file_path, file_length, slices, state
                    │
                    └─→ FilereadWorkerPool::submitTask (transfer_task.cpp:50)
                            │
                            │   将任务放入 task_queue_ 队列 (行 59)
                            │   通知工作线程 (行 61)
                            │
                            └─→ FilereadWorkerPool::workerThread (transfer_task.cpp:64)
                                    │
                                    │   工作线程从队列取出任务 (行 68-85)
                                    │
                                    └─→ StorageBackend::LoadObject (transfer_task.cpp:97)
                                            │
                                            │   调用 backend_->LoadObject()
                                            │   文件位置: storage_backend.cpp:419
                                            │
                                            └─→ StorageBackend::LoadObject 实现 (storage_backend.cpp:419-496)
                                                    │
                                                    ├─→ ResolvePath(path) (行 421)
                                                    │       处理路径
                                                    │
                                                    ├─→ create_file(path, FileMode::Read) (行 422)
                                                    │       创建 StorageFile 实例
                                                    │
                                                    └─→ StorageFile::vector_read (行 440)
                                                            使用 scatter-gather I/O 读取数据到 slices
```

## 内存副本调用栈（不经过 StorageBackend）

对于内存副本，数据传输不经过 StorageBackend：

### LOCAL_MEMCPY 策略（本地节点）

```
TransferSubmitter::submit (transfer_task.cpp:438)
    │
    │   selectStrategy() 返回 LOCAL_MEMCPY
    │
    └─→ TransferSubmitter::submitMemcpyOperation (transfer_task.cpp:545)
            │
            └─→ MemcpyWorkerPool::submitTask (transfer_task.cpp:582)
                    │
                    └─→ MemcpyWorkerPool::workerThread
                            │
                            └─→ std::memcpy (行 195)
                                    直接内存拷贝
```

### TRANSFER_ENGINE 策略（远程节点）

```
TransferSubmitter::submit (transfer_task.cpp:438)
    │
    │   selectStrategy() 返回 TRANSFER_ENGINE
    │
    └─→ TransferSubmitter::submitTransferEngineOperation (transfer_task.cpp:625)
            │
            ├─→ engine_.openSegment() (行 633)
            │       打开传输端点
            │
            └─→ TransferSubmitter::submitTransfer (transfer_task.cpp:590)
                    │
                    ├─→ engine_.allocateBatchID() (行 594)
                    │
                    └─→ engine_.submitTransfer() (行 601)
                            通过 RDMA/TCP 等传输引擎传输数据
```

## 关键代码位置

| 组件 | 文件 | 行号 | 功能 |
|------|------|------|------|
| Client::BatchGet | client_service.cpp | 871 | 批量获取入口 |
| TransferSubmitter::submit | transfer_task.cpp | 438 | 根据 replica 类型选择策略 |
| replica 类型判断 | transfer_task.cpp | 443-466 | 分流到不同处理路径 |
| submitFileReadOperation | transfer_task.cpp | 664-679 | 创建文件读取任务 |
| FilereadWorkerPool::submitTask | transfer_task.cpp | 50-62 | 提交任务到工作队列 |
| FilereadWorkerPool::workerThread | transfer_task.cpp | 64-117 | 工作线程处理任务 |
| StorageBackend::LoadObject | storage_backend.cpp | 419-496 | 从磁盘读取数据 |

## Replica 类型判断逻辑

```cpp
// transfer_task.cpp:443-466
if (replica.is_memory_replica()) {
    // 内存副本：使用 TransferEngine 或 MemcpyWorkerPool
    TransferStrategy strategy = selectStrategy(handle, slices);
    switch (strategy) {
        case TransferStrategy::LOCAL_MEMCPY:
            future = submitMemcpyOperation(handle, slices, op_code);
            break;
        case TransferStrategy::TRANSFER_ENGINE:
            future = submitTransferEngineOperation(handle, slices, op_code);
            break;
    }
} else {
    // 磁盘副本：使用 FilereadWorkerPool → StorageBackend
    future = submitFileReadOperation(replica, slices, op_code);
}
```

## StorageBackend::LoadObject 实现

```cpp
// storage_backend.cpp:419-496
tl::expected<void, ErrorCode> StorageBackend::LoadObject(
    const std::string& path, std::vector<Slice>& slices, int64_t length) {
    
    ResolvePath(path);  // 处理路径
    auto file = create_file(path, FileMode::Read);  // 打开文件
    
    // 使用 vector_read 进行 scatter-gather I/O
    // 将文件数据直接读取到多个 slice buffer 中
    auto read_result = file->vector_read(
        iovs_chunk.data(), static_cast<int>(iovs_chunk.size()),
        chunk_start_offset);
    
    return {};
}
```

## 组件关系图

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ BatchGet()                                                   ││
│  │   ├─ BatchQuery() → MasterClient → 获取 replica 信息        ││
│  │   ├─ FindFirstCompleteReplica()                             ││
│  │   └─ TransferSubmitter::submit()                            ││
│  └─────────────────────────────────────────────────────────────┘│
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                     TransferSubmitter                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ Memory Replica  │  │ Memory Replica  │  │  Disk Replica   │  │
│  │ (本地节点)      │  │ (远程节点)      │  │                 │  │
│  │                 │  │                 │  │                 │  │
│  │ MemcpyWorkerPool│  │ TransferEngine  │  │FilereadWorkerPool│ │
│  │     ↓           │  │     ↓           │  │     ↓           │  │
│  │ std::memcpy     │  │ RDMA/TCP        │  │ StorageBackend  │  │
│  └─────────────────┘  └─────────────────┘  │     ↓           │  │
│                                            │ LoadObject()    │  │
│                                            │     ↓           │  │
│                                            │ File I/O        │  │
│                                            └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      StorageBackend                              │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ LoadObject(path, slices, length)                            ││
│  │   ├─ create_file() → StorageFile                           ││
│  │   └─ vector_read() → scatter-gather I/O                    ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## 性能优化点

1. **FilereadWorkerPool**: 使用多线程（默认 10 个）并行读取磁盘数据，充分利用 SSD 带宽
2. **vector_read**: 使用 scatter-gather I/O，减少系统调用次数
3. **异步提交**: TransferSubmitter 立即返回 Future，不阻塞主线程
4. **批量处理**: BatchGet 支持批量提交传输任务，提高吞吐量

## 相关配置

- `MC_STORE_MEMCPY`: 环境变量，控制是否启用本地 memcpy 优化（默认禁用）
- `kDefaultFilereadWorkers`: 默认 10 个文件读取工作线程
- `kDefaultMemcpyWorkers`: 默认 1 个 memcpy 工作线程（受内存带宽限制）