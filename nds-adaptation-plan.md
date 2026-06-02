# Mooncake 盘框（NDS）适配方案

## 概述

本方案将 Mooncake Store 的磁盘持久化路径从传统文件系统（StorageBackend）替换为盘框 KV 存储（NDS），实现零拷贝的磁盘读写，同时保持与非盘框部署的兼容性。核心思路：引入 `use_od` 标志作为双后端路由开关，NDS 初始化从 `Client::Create` 延迟到 `RegisterLocalMemory` 触发，异步批量写入提升吞吐。

---

## 一、新增文件

| 文件路径 | 说明 |
|---|---|
| `mooncake-store/include/kv_storage_backend.h` | KVStorageBackend 类声明：Init/CleanupNDS/isInitialized/StoreObjects/LoadObjects/Remove 等 |
| `mooncake-store/src/kv_storage_backend.cpp` | 310 行完整实现：NDSLoader（dlopen 动态加载 libndskv.so + nsid 参数）、batch Put/Get 零拷贝、contiguous 校验、计时日志 |
| `mooncake-store/tests/nds_client_test.cpp` | 537 行 C++ 单元测试：NDS Client 全流程（创建/注册内存/Put/Get/批量操作） |
| `mooncake-store/tests/nds_stress_test.py` | 643 行 Python 压力测试 |
| `mooncake-store/tests/nds_thread_stress_test.py` | 598 行 Python 多线程压力测试 |

---

## 二、删除文件

| 文件路径 | 说明 |
|---|---|
| `mooncake-store/include/nds/nds_mock.cpp` | 旧的文件系统 mock 实现（210 行），已被 KVStorageBackend + NDSLoader 替代 |
| `mooncake-store/include/nds/nds_mock_test_utils.cpp` | mock 测试辅助函数 |
| `mooncake-store/include/nds/nds_mock_test_utils.h` | mock 测试辅助头文件 |
| `mooncake-store/include/nds/test_nds_mock.cpp` | mock 单元测试（155 行） |
| `mooncake-store/src/nds_c_bridge.cpp` | 多余的 C bridge 文件：NDS 动态库已导出 `extern "C"` 符号（init/batchGet/batchPut 等），NDSLoader 直接 dlsym 加载这些符号，无需额外桥接层 |
| `mooncake-store/include/nds/nds_c_wrapper.cpp` | 同上，与 nds_c_bridge.cpp 功能完全重复且均无必要，已删除 |

---

## 三、核心架构变更

### 3.1 NDS 初始化流程重构

**kv-main 原流程：**
```
Client::Create(... nds_mem_addr, nds_mem_size ...)
  → PrepareStorageBackend(... nds_mem_addr, nds_mem_size ...)
    → KVStorageBackend::Init(nds_mem_addr, nds_mem_size)   // 在 Create 时立即初始化 NDS
```

**kv_v5 新流程（两个独立调用）：**

步骤一：Client::Create
```
Client::Create(...)                                    // 不传 nds_mem 参数
  → GetStorageConfig → use_od_=true
  → PrepareStorageBackend(...)                         // 只创建 KVStorageBackend，不调 Init
```

步骤二：Client::RegisterLocalMemory（独立调用，非 Create 内部）
```
Client::RegisterLocalMemory(addr, length, ...)
  → if use_od_ && kv_storage_backend_ && !isInitialized()
    → kv_storage_backend_->Init(addr, length)         // NDS init 由 register_buffer 触发
```

**关键改动点：**

| 文件 | 改动 |
|---|---|
| `client_service.h` | 新增 `kv_storage_backend_` 成员、`use_od_` bool、`HasDiskStorage()` 方法 |
| `client_service.cpp` | `PrepareStorageBackend` 不调 Init；`RegisterLocalMemory` 加 use_od_ NDS init 检查；`unregisterLocalMemory` 加 use_od_ CleanupNDS 检查 |
| `real_client.cpp` | 新增空 `ConfigDict` `{}`；`setup_real` 传入 `50052, false`（rpc_port, use_od） |
| `dummy_client.h` | `setup_real` 签名移除 nds_mem 参数 |

### 3.2 NDS 释放流程

**新流程：**
```
Client::unregisterLocalMemory(addr, ...)
  → if use_od_ && kv_storage_backend_ && isInitialized()
    → kv_storage_backend_->CleanupNDS()
      → free(owns_nds_memory_) + 重置 NDSLoader 全局状态 + initialized_=false
```

`CleanupNDS()` 同时重置：
- 本地状态：`owns_nds_memory_`、`nds_mem_addr_`、`nds_mem_size_`、`initialized_`
- 全局状态：`NDSLoader::Instance()` 的 `nds_initialized`、`nds_mem_addr`、`nds_mem_size`

析构函数 `~KVStorageBackend()` 也调 `CleanupNDS()`。

### 3.3 use_od 双后端路由

`use_od_` 是从 Master 的 `GetStorageConfig` RPC 获取的布尔标志，贯穿 Client 全生命周期：

| 操作 | use_od_=true | use_od_=false |
|---|---|---|
| 磁盘存储后端创建 | `KVStorageBackend` | `StorageBackend` |
| `HasDiskStorage()` | `kv_storage_backend_ != nullptr` | `storage_backend_ != nullptr` |
| Put 磁盘 replica | `kv_storage_backend_->StoreObjects` | `PutToLocalFile`（异步文件写入） |
| BatchPut 磁盘 replica | 整批异步 `StoreObjects + BatchPutEndDisk` | 逐 key `PutToLocalFile` |
| Get 磁盘 replica | `kv_storage_backend_->LoadObjects`（FilereadTask NDS 路径） | `backend_->LoadObject`（FilereadTask 文件路径） |
| Remove | `kv_storage_backend_->Remove` | `storage_backend_->RemoveFile` |
| RemoveByRegex | `kv_storage_backend_->RemoveByRegex` | `storage_backend_->RemoveByRegex` |
| RemoveAll | `kv_storage_backend_->RemoveAll` | `storage_backend_->RemoveAll` |

### 3.4 Remove/RemoveByRegex/RemoveAll 后端路由

**kv-main 原实现（后端调用被注释掉）：**
```cpp
// Client::Remove:
auto result = master_client_.Remove(key, force);
// if (storage_backend_) {
//     storage_backend_->RemoveFile(key);   // ← 被注释掉，本地清理不执行
// }

// Client::RemoveByRegex / RemoveAll：同理，storage_backend_ 调用全部被注释掉
```

**kv_v5 新实现（use_od_ 双后端路由，重新启用本地清理）：**
```cpp
// Client::Remove:
auto result = master_client_.Remove(key, force);
if (use_od_) {
    if (kv_storage_backend_) kv_storage_backend_->Remove(key);   // NDS 本地删除
} else {
    if (storage_backend_) storage_backend_->RemoveFile(key);     // 文件系统本地删除
}

// Client::RemoveByRegex / RemoveAll：同样的 use_od_ 分支路由
```

**关键变化：**
- kv-main 中本地后端清理（RemoveFile/RemoveByRegex/RemoveAll）被注释掉，导致 Remove 只删 master 元数据，不删本地数据
- kv_v5 重新启用本地清理，并根据 `use_od_` 路由到 NDS 或文件系统后端
- NDS 路径：`kv_storage_backend_->Remove(key)` → 调用 NDSLoader C API 删除单个对象
- NDS 路径：`kv_storage_backend_->RemoveByRegex(str)` → 按正则批量删除 NDS 对象
- NDS 路径：`kv_storage_backend_->RemoveAll()` → 清空所有 NDS 对象

### 3.5 BatchPut NDS 异步化

**kv-main 原流程（同步文件写入）：**
```
SubmitTransfers:
  for each op:
    if disk replica → PutToLocalFile(key, slices, disk_descriptor)   // 同步写文件 + 异步 PutEnd
    for memory replicas → submit TransferWrite
WaitForTransfers:
  等待所有 TransferFuture
FinalizeBatchPut:
  BatchPutEnd(MEMORY)
```

**kv_v5 新流程（NDS 异步批量写入）：**
```
SubmitTransfers:
  if use_od_:
    收集所有 disk replica keys/slices
    → write_thread_pool_.enqueue([StoreObjects + BatchPutEndDisk/PutRevoke])
    → 每个 key 挂 MemcpyOperationState future（加入 op.pending_transfers）
  if !use_od_ && storage_backend_:
    逐 key PutToLocalFile
  for memory replicas → submit TransferWrite（所有 ops）
WaitForTransfers:
  等待所有 TransferFuture（NDS future + 内存 transfer future 并发等待）
FinalizeBatchPut:
  BatchPutEnd(MEMORY)   // 只处理 MEMORY replica
```

**关键设计：**
- NDS 写入整批提交（`StoreObjects` 一次调用），而非逐 key
- 每个 key 的 NDS future 挂在 `op.pending_transfers` 上，`WaitForTransfers` 可统一等待
- `BatchPutEndDisk` 在异步 lambda 内调用，不阻塞主线程
- 失败时调 `PutRevoke(DISK)` 撤销

### 3.6 单键 Put NDS 路径

`Client::Put()` 方法新增 `use_od_` 分支：
```cpp
if (use_od_) {
    // 找到 disk replica → kv_storage_backend_->StoreObjects({key}, {{slices}})
    // 成功 → BatchPutEndDisk({key})
    // 失败 → PutRevoke(key, DISK)
} else if (storage_backend_) {
    // 找到 disk replica → PutToLocalFile(key, slices, disk_descriptor)
}
// memory replicas → TransferWrite（不变）
```

### 3.7 TransferSubmitter 扩展

| 改动 | 说明 |
|---|---|
| 构造函数新增参数 | `std::shared_ptr<KVStorageBackend>& kv_backend`、`bool use_od` |
| `FilereadWorkerPool` | 新增 `kv_backend_` 成员；worker 线程根据 `task.use_nds` 分流到 NDS 或文件路径 |
| `FilereadTask` | 新增 `use_nds` bool + `nds_key` string；两种构造函数（文件路径 vs NDS key） |
| `submitBatchMemcpyOperation` | 新增方法：批量 memcpy 操作，用于 BatchGet 同节点优化 |
| `MemcpyWorkerPool` | worker 数量从 1 → 4；可通过 `MC_MEMCPY_WORKERS` 环境变量配置（1~64） |
| `TransferFuture` | 从不可拷贝改为可拷贝（支持 batch future 管理，`pending_transfers` 需要 copy） |
| `EmptyOperationState` | 构造时设置 `result_=ErrorCode::OK`（NDS future 完成后需有效默认状态） |

### 3.8 BatchGet NDS 整批读取

**kv-main 原流程（逐key提交）：**
```
BatchGet:
  for each key:
    FindFirstCompleteReplica → 找到 replica
    → TransferSubmitter::submit(replica, slices, READ)
      if MEMORY replica → LOCAL_MEMCPY / TRANSFER_ENGINE
      if DISK replica → submitFileReadOperation → FilereadTask
  WaitForTransfers → 逐key等待 future.get()
```

**kv_v5 新流程（use_od_=true，整批提交）：**
```
BatchGet (use_od_=true):
  逐key查找replica → 收集 batch_replicas / batch_slices / batch_indices
  → submit_batch(batch_replicas, batch_slices, READ)
    → replicas[0].is_disk_replica() && use_od_
      → submitBatchFileReadOperation
        → 提取 nds_keys = 各replica.get_disk_descriptor().file_path
        → 创建 BatchFilereadTask(nds_keys, batched_slices, state)
        → fileread_pool_->submitBatchTask(task)
        → return TransferFuture(state)
    → 否则 → 原有 MEMORY 路径
  → future->get() ← 阻塞等待整批完成
    → FilereadWorkerPool workerThread:
      → kv_backend_->LoadObjects(nds_keys, batched_slices) ← 一次性NDS批量读取
      → state->set_completed(OK / TRANSFER_FAIL)
  → 所有 batch_indices 统一设置成功或失败
```

**逻辑变更：**
- `BatchGet` 新增 `use_od_` 分支：NDS 模式不再逐key调 `submit`，而是收集所有replica后一次性调 `submit_batch`
- 整批提交意味着一次 `LoadObjects` 调用处理所有key，而非逐key `LoadObjects({key}, {slices})`
- 所有key共享同一个 `TransferFuture`：整批成功或整批失败
- 无效key（找不到replica / 无slices）在收集阶段过滤，不参与batch提交

**Transfer层新增：**

| 改动 | 文件 | 说明 |
|---|---|---|
| `BatchFilereadTask` struct | `transfer_task.h` | 批量NDS读取任务，含 `nds_keys` + `batched_slices` + `state` |
| `FilereadTaskVariant` | `transfer_task.h` | `std::variant<FilereadTask, BatchFilereadTask>`，FilereadWorkerPool队列类型 |
| `FilereadWorkerPool::submitBatchTask()` | `transfer_task.h/cpp` | 批量任务入队方法 |
| `FilereadWorkerPool::workerThread()` | `transfer_task.cpp` | 从取 `FilereadTask` 改为取 `FilereadTaskVariant`，`std::holds_alternative` 分发到单键/批量两条路径 |
| `TransferSubmitter::submitBatchFileReadOperation()` | `transfer_task.h/cpp` | 批量NDS读取提交：提取NDS keys → 创建 `BatchFilereadTask` → 提交到 `fileread_pool_` → 返回 `TransferFuture` |
| `TransferSubmitter::submit_batch()` | `transfer_task.cpp` | 新增前置判断：`replicas[0].is_disk_replica() && use_od_` → 调 `submitBatchFileReadOperation` |

### 3.9 Master 端变更

**master_config.h：** `use_od` 字段贯穿全链路
- `MasterConfig`、`MasterServiceConfig`、`MasterServiceConfigBuilder`、`InProcMasterConfig`、`InProcMasterConfigBuilder`、`WrappedMasterServiceConfig` 全部新增 `use_od`

**master_service.cpp：**
- `use_od_=true` → 强制 `use_disk_replica_=true`（即使 `root_fs_dir_` 为空）
- `PutStart` DISK replica 分配：
  - `root_fs_dir_` 非空 → 生成 `file_path = ResolvePathFromKey(key, root_fs_dir_, cluster_id_)`
  - `root_fs_dir_` 为空 → `file_path = ""`（盘框场景不需要文件路径）
- 新增 `BatchPutEndDisk` 方法：批量调用 `PutEnd(client_id, key, ReplicaType::DISK)`
- `GetStorageConfig` 返回新增 `use_od` 字段

**rpc_service.h/cpp：**
- `WrappedMasterService` 新增 `BatchPutEndDisk` RPC 方法
- RPC 注册新增 `BatchPutEndDisk` handler

**rpc_types.h：**
- `GetStorageConfigResponse` 新增 `use_od` 字段 + 构造函数参数 + YLT_REFL 序列化

**master_client.h/cpp：**
- `MasterClient` 新增 `BatchPutEndDisk(keys)` 方法
- 新增 `RpcNameTraits<BatchPutEndDisk>` 特化

**master.cpp：**
- 启动时传入 `use_od` 配置

### 3.10 Python binding 变更

**store_py.cpp：**
- `setup` 绑定移除 `nds_mem_addr/nds_mem_size` 参数
- 新增三个 glog 控制绑定：
  - `init_glog(argv0)` — 初始化 glog
  - `set_vlog_level(level)` — 设置 VLOG 级别
  - `set_log_to_stderr(enabled)` — 日志输出到 stderr

### 3.11 NDS 接口重构

**nds_interface.h：**
- 移除注释文档，新增 `batchGet/batchPut` 方法签名
- 新增 `extern "C"` 声明（init/get/put/batchGet/batchPut）
- **所有 get/put/batchGet/batchPut 签名新增 `uint32_t nsid` / `const uint32_t* nsids` 参数**
- `isExists` 签名改为 `(const uint64_t*, size_t)`（原为 `(uint64_t*, int32_t)`）
- `batchGet/batchPut` 最后参数改为 `size_t count`（原为 `int32_t`），新增 `const uint32_t* nsids` 参数

**NDSLoader typedefs 对应更新：**
```cpp
typedef int32_t (*NDS_get_fn)(uint64_t, uint8_t*, size_t, size_t, uint32_t);
typedef int32_t (*NDS_put_fn)(uint64_t, uint8_t*, size_t, size_t, uint32_t);
typedef int32_t (*NDS_batchGet_fn)(const uint64_t*, uint8_t**, const size_t*,
                                   const size_t*, const uint32_t*, uint32_t);
typedef int32_t (*NDS_batchPut_fn)(const uint64_t*, uint8_t**, const size_t*,
                                   const size_t*, const uint32_t*, uint32_t);
```

**nsid 传递机制：**
- 环境变量 `MC_NDS_NSID`（uint32_t 十进制字符串）在 `Client::Create` 中读取
- `PrepareStorageBackend` 创建 `KVStorageBackend` 后立即调用 `setNsid(nsid_val)` 保存
- `KVStorageBackend` 内部成员 `nsid_`：每次调用 `batchPut/batchGet` 时自动填充 `std::vector<uint32_t> nsids(count, nsid_)` 传给 NDSLoader
- `LoadObjects` 在 `FilereadWorkerPool::workerThread` 中调用——kv_storage_backend_ 已持有 nsid_，无需 FilereadTask/BatchFilereadTask 传递

**KVStorageBackend 新增：**
```cpp
void setNsid(uint32_t nsid);
uint32_t nsid() const;
// private:
uint32_t nsid_{0};
```

### 3.12 CMake 变更

**src/CMakeLists.txt：**
- 新增 `kv_storage_backend.cpp` 到源文件列表
- 移除 `ndsclient` 库链接（不再依赖静态编译的 mock 库）

**tests/CMakeLists.txt：**
- 新增 `nds_client_test` 测试目标

---

## 四、StoreObjects/LoadObjects 零拷贝设计

NDS 要求零拷贝：源数据地址必须在 NDS init 内存区域内。

**StoreObjects 流程：**
```
1. 每个 key 的 slices 做 contiguous 校验（相邻 slice 的 ptr + size == 下一个 slice 的 ptr）
2. contiguous → 合并为单个 NDS entry（blockId, 首slice.ptr, offset=0, total_size）
3. 不 contiguous → 返回 INVALID_PARAMS（拒绝 memcpy 路径）
4. 调用 NDSLoader::batchPut(blockIds, blockAddrs, offsets, lengths, count)
```

**LoadObjects 流程：** 同理，contiguous 校验 + batchGet

**objectKeyToUint64：** 将字符串 key 转为 uint64_t blockId（NDS 的 KV key）

---

## 五、KVStorageBackend 类结构

```cpp
class KVStorageBackend {
public:
    Init(void* nds_mem_addr = nullptr, uint64_t nds_mem_size = 0);  // 初始化 NDS
    CleanupNDS();                                                     // 释放 NDS 资源
    isInitialized() const;                                            // atomic 检查
    StoreObjects(keys, batched_slices);                               // 批量写入
    LoadObjects(keys, batched_slices);                                // 批量读取
    Remove(key);                                                      // NDS 不支持删除，空操作
    RemoveByRegex(key);                                               // NDS 不支持正则删除
    RemoveAll();                                                      // NDS 不支持全清

private:
    bool owns_nds_memory_{false};      // 是否拥有 NDS 内存（self-allocated）
    void* nds_mem_addr_ = nullptr;
    uint64_t nds_mem_size_ = 0;
    std::atomic<bool> initialized_{false};
};
```

**NDSLoader（全局单例）：**
```cpp
struct NDSLoader {
    void* handle;                      // dlopen handle
    NDS_init_fn / isExists / get / put / batchGet / batchPut;  // 动态加载的函数指针
    bool nds_initialized;              // 全局初始化状态
    void* nds_mem_addr;                // 全局 NDS 内存地址
    uint64_t nds_mem_size;

    static NDSLoader& Instance();      // 进程级单例
    bool Load();                       // dlopen libndskv.so
    void Unload();                     // dlclose
};
```

- NDS 是进程级全局单例（C API 无 context/handle 参数），不支持多实例
- `NDS_LIBRARY_PATH` 环境变量指定 so 路径，默认 `libndskv.so`
- Init 时：有外部内存 → 用用户提供的地址；无外部内存 → `aligned_alloc(4096, 1GB)` 自分配

---

## 六、新增计时日志

为性能分析添加了大量计时日志（`std::chrono::steady_clock`），标记格式 `[函数名]`：

| 位置 | 日志标记 |
|---|---|
| `StartBatchPut` | `[StartBatchPut] BatchPutStart RPC: X us for N keys` |
| `SubmitTransfers` | `[SubmitTransfers] NDS enqueue: X us, N keys queued` |
| `SubmitTransfers` | `[SubmitTransfers] total: X us` |
| NDS Lambda | `[NDS Lambda] StoreObjects: X us for N keys` |
| NDS Lambda | `[NDS Lambda] BatchPutEndDisk RPC: X us (cumulative)` |
| `KVStoreObjects` | `[BatchPut] KVStoreObjects flatten slices: X us` |
| `KVStoreObjects` | `[BatchPut] KVStoreObjects NDS batchPut call: X us` |
| `WaitForTransfers` | `[WaitForTransfers] future[i] key=X: X us, result=Y` |
| `WaitForTransfers` | `[WaitForTransfers] total: X us` |
| `FinalizeBatchPut` | `[FinalizeBatchPut] BatchPutEnd(MEMORY) RPC: X us` |
| `FinalizeBatchPut` | `[FinalizeBatchPut] total: X us` |
| `BatchPut` | `[BatchPut] SubmitTransfers / WaitForTransfers / FinalizeBatchPut / TOTAL` |

---

## 七、关键设计决策

| 决策 | 原因 |
|---|---|
| NDS init 由 RegisterLocalMemory 触发 | 零拷贝要求源数据地址在 NDS 内存区域内；register_buffer 时才知道可用内存 |
| 不使用单独 InitNDS 方法 | 用户明确要求复用 `Init(void*, uint64_t)` 签名 |
| NDSLoader 全局单例 | NDS C API 全是全局状态操作，不支持多实例 |
| nsid 存储在 KVStorageBackend 中 | NDSLoader 是全局单例无法存储 per-client 状态；KVStorageBackend 与 Client 1:1 绑定，StoreObjects/LoadObjects 内部自动填充 nsids |
| MC_NDS_NSID 环境变量在 Client::Create 中读取 | nsid 是部署级配置，不暴露给上层 API；Client::Create 调 PrepareStorageBackend 时一次性设置 |
| use_od=true 无 root_fs_dir 时 master 返回 DISK replica | 盘框场景不需要文件路径，file_path 为空字符串 |
| CleanupNDS() 是 public 方法 | 供 unregisterLocalMemory 和析构函数调用 |
| batchPut 整批异步提交 | 减少 RPC 调用次数，StoreObjects 一次处理所有 disk key |
| TransferFuture 可拷贝 | NDS batch 写入需要将 MemcpyOperationState future 加入 op.pending_transfers |
| MemcpyWorkerPool 4线程 | memcpy 并发度提升，可通过 MC_MEMCPY_WORKERS 配置 |

---

## 八、完整流程图

### 8.1 NDS 初始化流程

```
Python: store.setup(use_od=True)
  ├─ Client::Create(...)
  │    ├─ GetStorageConfig → use_od_=true
  │    └─ PrepareStorageBackend → 创建 KVStorageBackend（不调 Init）
  │
  └─ Python: store.register_buffer(ptr, size)
       ├─ Client::RegisterLocalMemory(ptr, size, ...)
       │    ├─ use_od_=true && kv_storage_backend_ && !isInitialized()
       │    │    ├─ KVStorageBackend::Init(ptr, size)
       │    │    │    ├─ NDSLoader::Load() → dlopen("libndskv.so")
       │    │    │    ├─ NDSLoader::init(ptr, size) → NDS C API init
       │    │    │    └─ initialized_ = true
       │    │    │
       │    │    └─ use_od_=false / 已初始化 → 正常注册内存
```

### 8.2 NDS 释放流程

```
Python: store.unregister_buffer(ptr)
  ├─ Client::unregisterLocalMemory(ptr, ...)
  │    ├─ use_od_=true && kv_storage_backend_ && isInitialized()
  │    │    ├─ KVStorageBackend::CleanupNDS()
  │    │    │    ├─ if owns_nds_memory_ → free(nds_mem_addr_)
  │    │    │    ├─ 重置 NDSLoader 全局状态
  │    │    │    └─ initialized_ = false
  │    │    │
  │    │    └─ use_od_=false / 未初始化 → 正常注销内存
  │
  └─ 或析构路径：
       └─ ~KVStorageBackend() → CleanupNDS()
```

### 8.3 NDS BatchPut 流程

```
Python: store.batch_put(keys, values)
  ├─ StartBatchPut → BatchPutStart RPC
  │
  ├─ SubmitTransfers
  │    ├─ NDS 路径（use_od_=true，所有 DISK replica key）
  │    │    ├─ 收集所有 disk key
  │    │    ├─ write_thread_pool_.enqueue:
  │    │    │    ├─ StoreObjects(keys, slices) → NDS batchPut C API
  │    │    │    ├─ 成功 → BatchPutEndDisk(keys)
  │    │    │    └─ 失败 → PutRevoke(key, DISK) per key
  │    │    └─ 每个 key 挂 MemcpyOperationState future
  │    │
  │    ├─ Memory 路径（所有 MEMORY replica ops）
  │    │    ├─ TransferWrite → TransferEngine batch submit
  │    │    └─ 每个 key 挂 TransferEngine future
  │
  ├─ WaitForTransfers → 等待 NDS future + Memory future
  │
  ├─ FinalizeBatchPut
  │    ├─ 成功的 keys → BatchPutEnd(MEMORY)
  │    ├─ 失败的 keys → BatchPutRevoke
  │    │    ├─ PutRevoke(MEMORY)
  │    │    ├─ PutRevoke(DISK)
  │    │    └─ 任一成功即返回成功，都失败才返回错误
```

### 8.4 NDS BatchGet 流程

```
Python: store.batch_get(keys)
  ├─ QueryBatch → BatchQuery RPC → 返回 replica 列表
  │
  ├─ BatchGet (use_od_=true)
  │    ├─ 逐key查找replica
  │    │    ├─ slices.find(key) → 失败 → results[i] = INVALID_PARAMS, 跳过
  │    │    ├─ FindFirstCompleteReplica → 失败 → results[i] = err, 跳过
  │    │    └─ 成功 → 加入 batch_replicas / batch_slices / batch_indices
  │    │
  │    ├─ submit_batch(batch_replicas, batch_slices, READ)
  │    │    ├─ replicas[0].is_disk_replica() && use_od_
  │    │    │    ├─ submitBatchFileReadOperation
  │    │    │    │    ├─ 提取 nds_keys = 各replica.get_disk_descriptor().file_path
  │    │    │    │    ├─ 创建 BatchFilereadTask(nds_keys, batched_slices, state)
  │    │    │    │    ├─ fileread_pool_->submitBatchTask(task)
  │    │    │    │    └─ return TransferFuture(state)
  │    │    │    │
  │    │    └─ 否则 → 原有 MEMORY 路径（LOCAL_MEMCPY / TRANSFER_ENGINE）
  │    │
  │    ├─ future->get() ← 阻塞等待整批完成
  │    │    ├─ FilereadWorkerPool workerThread 取出 BatchFilereadTask
  │    │    │    ├─ kv_backend_->LoadObjects(nds_keys, batched_slices)
  │    │    │    │    ← 一次性NDS批量读取所有key
  │    │    │    ├─ 成功 → state->set_completed(OK)
  │    │    │    └─ 失败 → state->set_completed(TRANSFER_FAIL)
  │    │
  │    ├─ batch_result == OK → 所有 batch_indices 的 results[idx] = {}
  │    ├─ batch_result != OK → 所有 batch_indices 的 results[idx] = TRANSFER_FAIL
  │    │
  │    └─ 检查 lease 过期 → return results
  │
  └─ BatchGet (use_od_=false)
       ├─ 逐key: submit(replica, slices)
       │    ├─ MEMORY replica → selectStrategy
       │    │    ├─ LOCAL_MEMCPY → submitMemcpyOperation
       │    │    └─ TRANSFER_ENGINE → submitTransferEngineOperation
       │    ├─ DISK replica → submitFileReadOperation → FilereadTask
       │    └─ per-key TransferFuture
       │
       ├─ 逐key等待 future.get()
       ├─ 逐key设置 results[i]
       └─ 检查 lease 过期 → return results
```

### 8.5 单键 Put 流程

```
Python: store.put(key, value)
  ├─ StartBatchPut(key) → BatchPutStart RPC
  │
  ├─ SubmitTransfers
  │    ├─ NDS 路径（use_od_=true）
  │    │    ├─ write_thread_pool_.enqueue:
  │    │    │    ├─ StoreObjects({key}, {slices}) → NDS batchPut C API
  │    │    │    ├─ 成功 → BatchPutEndDisk({key})
  │    │    │    └─ 失败 → PutRevoke(key, DISK)
  │    │    └─ 挂 MemcpyOperationState future
  │    │
  │    ├─ Memory 路径
  │    │    ├─ TransferWrite → TransferEngine submit
  │    │    └─ 挂 TransferEngine future
  │
  ├─ WaitForTransfers → 等待 NDS future + Memory future
  │
  ├─ FinalizeBatchPut
  │    ├─ 成功 → BatchPutEnd(MEMORY)
  │    ├─ 失败 → BatchPutRevoke
  │    │    ├─ PutRevoke(MEMORY)
  │    │    ├─ PutRevoke(DISK)
  │    │    └─ 任一成功即返回成功
```

### 8.6 单键 Get 流程

```
Python: store.get(key, value)
  ├─ Query(key) → Query RPC → 返回 replica 列表
  │
  ├─ Get (内部走 BatchGet({key}))
  │    ├─ use_od_=true
  │    │    ├─ FindFirstCompleteReplica → DISK replica
  │    │    ├─ submit_batch({replica}, {slices}, READ)
  │    │    │    ├─ submitBatchFileReadOperation
  │    │    │    │    ├─ BatchFilereadTask → LoadObjects({key}, {slices})
  │    │    │    │    └─ 单个 TransferFuture
  │    │    └─ future->get() → 等待 NDS 读取完成
  │    │
  │    └─ use_od_=false
  │         ├─ FindFirstCompleteReplica → MEMORY/DISK replica
  │         ├─ submit(replica, slices)
  │         │    ├─ MEMORY → LOCAL_MEMCPY / TRANSFER_ENGINE
  │         │    └─ DISK → submitFileReadOperation → FilereadTask
  │         └─ per-key future.get()
```

---

## 九、测试脚本详解

### 9.1 nds_client_test.cpp — C++ GTest 单元测试

**定位：** `mooncake-store/tests/nds_client_test.cpp`，537 行，基于 Google Test 框架。

**测试环境搭建：**
1. `SetUpTestSuite()`：创建 InProcMaster（进程内 Master，`use_od=true`），创建 Client + Segment Provider，分配 256MB SimpleAllocator buffer 并 `RegisterLocalMemory`（触发 NDS init）
2. `TearDownTestSuite()`：释放 buffer → CleanupClients → Stop Master → 删除临时目录

**关键配置：**
- Master 启动：`InProcMasterConfigBuilder().set_use_od(true).set_enable_disk_eviction(true).set_root_fs_dir(tmp_dir)`
- Client 创建：`Client::Create("localhost:17820", "P2PHANDSHAKE", FLAGS_protocol, std::nullopt, master_address_, nullptr, {})`
- 内存注册：`RegisterLocalMemory(buffer_allocator_->getBase(), 256MB, "cpu:0")` — **这是触发 NDS init 的关键步骤**
- Segment：512MB RAM buffer，由 segment_provider MountSegment

**命令行参数：**
```
--protocol=tcp|rdma       # 传输协议
--device_name=             # RDMA 设备名
--default_kv_lease_ttl=N   # KV 对象租约 TTL（默认 30s）
```

**环境变量：**
```
PROTOCOL                  # 覆盖 --protocol
DEVICE_NAME               # 覆盖 --device_name
DEFAULT_KV_LEASE_TTL      # 覆盖 lease TTL
```

**运行命令：**
```bash
cd build
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
  ./mooncake-store/tests/nds_client_test --protocol=tcp
```

**测试用例一览：**

| 测试名 | 测试逻辑 | 数据规模 |
|---|---|---|
| `PutAndGetSingleKey` | 单 key Put → Get → memcmp 校验数据完整性 | 1MB |
| `PutAndGetMultiSliceKey` | 单 key 多 Slice（2 slice）Put → Get → 逐 slice memcmp | 1MB + 2MB |
| `BatchPutAndBatchGet` | 批量 10 key BatchPut → BatchGet → 逐 key memcmp | 10 × 1MB |
| `BatchPutThenIndividualGet` | 批量 5 key BatchPut → 逐 key 单独 Get → memcmp | 5 × 2MB |
| `IndividualPutThenBatchGet` | 逐 key 单独 Put 5 次 → BatchGet 全部 → memcmp | 5 × 2MB |
| `BatchPutWithMultiSlicePerKey` | 批量 3 key + 每个 key 2 slice → BatchPut → BatchGet → 逐 slice memcmp | 3 × (1+2)MB |
| `OverwriteExistingKey` | Put 1MB → Remove → Put 2MB → Get → 校验第二次数据 | 1MB → 2MB |
| `GetNonExistentKey` | Get 一个不存在 key → 验证返回错误 | 1MB buf |
| `LargePayloadPutAndGet` | 16MB 单 key Put → Get → memcmp | 16MB |

**测试覆盖的关键路径：**
- 单 key Put/Get（NDS StoreObjects + LoadObjects）
- 多 Slice contiguous 写入（验证 NDS 零拷贝 contiguous 校验）
- BatchPut 异步批量路径（NDS batchPut + BatchPutEndDisk RPC）
- BatchGet 批量读取路径（NDS batchGet via FilereadTask）
- Put + 单独 Get 交叉验证
- 单独 Put + BatchGet 交叉验证
- 覆写场景（Remove + 重 Put）
- 不存在 key 的错误处理
- 大 payload（16MB）

---

### 9.2 nds_stress_test.py — Python 多进程压力测试

**定位：** `mooncake-store/tests/nds_stress_test.py`，643 行。

**测试架构：** 多进程模型 — 每个 Worker 是独立进程，各自拥有独立的 MooncakeDistributedStore + NDS 实例。通过 `multiprocessing.Queue` 收集统计信息，主进程监控并汇总。

**执行流程：**
1. **Phase I — 启动 Master**：`mooncake_master --use_od=true` 子进程，自动分配 RPC/HTTP/Metrics 端口
2. **Phase I — Spawn Worker 进程**：每个 Worker 进程独立 setup + register_buffer + NDS init
3. **Phase II — 压力测试**：Worker 在 `duration` 秒内持续执行 batch_put/batch_get 操作
4. **Phase III — 停止 & 汇总**：收集最终统计数据，输出 Final Report

**Worker 进程内部逻辑：**
```
1. mmap 分配 buffer (batch_size × block_size)
2. MooncakeDistributedStore.setup(...)
3. register_buffer → 触发 NDS init
4. 循环（直到 stop_event 或 duration）：
   a. 生成 batch_keys (w{worker}_s{slot}_k{idx})
   b. batch_put_from / batch_get_into
   c. 如果 depth>0：环形复用 key slot
   d. 如果 eviction_window>0：超过窗口时 remove_by_regex 清理旧 slot
   e. 通过 stats_queue 上报结果
5. 清理：remove_by_regex 所有 alive slot → unregister_buffer → close mmap
```

**命令行参数：**

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--operation-mode` | `batch_put` | 操作模式：`batch_put`/`batch_get`/`mixed` |
| `--block-size` | `128KB` | 单个 block 大小（字节） |
| `--batch-size` | `128` | 每次批量操作的 key 数量 |
| `--num-workers` | `8` | Worker 进程数（每个进程独立 NDS） |
| `--duration` | `30` | 测试持续时间（秒） |
| `--monitor-interval` | `1` | 监控报告间隔（秒） |
| `--protocol` | `tcp` | 传输协议 |
| `--device-name` | `""` | RDMA 设备名（逗号分隔多设备，Worker round-robin） |
| `--local-hostname` | `127.0.0.1:0` | 本地 hostname（端口 0 自动分配） |
| `--global-segment-size` | `4096` | 全局 segment 大小（MB） |
| `--depth` | `1` | 环形缓冲深度（slot 复用数）；depth=0 无限唯一 key |
| `--eviction-window` | `1` | 最大存活 batch 数；超过时 remove 最早 batch |
| `--core-bind-start` | `5` | CPU 绑核起始编号（-1 禁用） |
| `--master-binary` | `""` | mooncake_master 路径（自动检测） |

**Master 启动配置：**
```bash
mooncake_master \
  --use_od=true \
  --cluster_id=nds_stress \
  --enable_http_metadata_server=true \
  --rpc_address=127.0.0.1 \
  --rpc_port={auto} \
  --http_metadata_server_port={auto} \
  --default_kv_lease_ttl=500 \
  --rpc_thread_num={num_workers×2}
```

**运行命令示例：**
```bash
# 默认 8 进程 batch_put 压力测试
python nds_stress_test.py

# 自定义参数
python nds_stress_test.py \
  --operation-mode=batch_put \
  --block-size=262144 \
  --batch-size=64 \
  --num-workers=4 \
  --duration=60 \
  --depth=4 \
  --eviction-window=2 \
  --protocol=tcp

# RDMA 模式
python nds_stress_test.py \
  --protocol=rdma \
  --device-name=mlx5_0,mlx5_1 \
  --num-workers=8

# Mixed 模式（一半 Put 一半 Get）
python nds_stress_test.py --operation-mode=mixed
```

**输出指标：**
- 实时：每秒带宽（GB/s）、平均延迟、错误数、错误率
- 最终报告：总 ops、总 bytes、总 errors、错误率、平均带宽、峰值带宽、平均延迟

**核心设计特点：**
- **多进程隔离**：每个 Worker 独立进程，独立 NDS 实例，测试进程级 NDS 全局单例安全性
- **环形缓冲 + 淘汰窗口**：depth 控制 key 复用，eviction_window 控制内存压力，模拟真实场景的 Put/Remove 循环
- **CPU 绑核**：`os.sched_setaffinity` 绑定 Worker 到指定 CPU，减少调度干扰
- **RDMA 多设备 round-robin**：多 RDMA 设备时 Worker 按序分配设备
- **统计防抖**：`snapshot_and_reset` 累积空窗口时间，避免突发带宽跳变

---

### 9.3 nds_thread_stress_test.py — Python 多线程压力测试

**定位：** `mooncake-store/tests/nds_thread_stress_test.py`，598 行。

**测试架构：** 多线程模型 — 单进程内所有线程共享一个 MooncakeDistributedStore 实例（共享 NDS 全局单例）。通过 `ThreadStats`（`threading.Lock` 保护）收集统计信息。

**与 nds_stress_test.py 的核心区别：**

| 对比项 | nds_stress_test.py | nds_thread_stress_test.py |
|---|---|---|
| 并发模型 | 多进程（`multiprocessing.Process`） | 多线程（`threading.Thread`） |
| Store/NDS 实例 | 每个 Worker 独立 Store + 独立 NDS | 所有线程共享同一 Store + 同一 NDS |
| Buffer 分配 | 每个 Worker 独立 mmap | 主进程一次 mmap，按偏移分给各线程 |
| 统计收集 | `multiprocessing.Queue` 跨进程 | `ThreadStats` + `threading.Lock` 进程内 |
| NDS 单例冲突 | 无冲突（独立进程） | 测试 NDS 全局单例在多线程下的安全性 |
| Key 前缀 | `w{worker}_s{slot}_k{idx}` | `t{thread}_s{slot}_k{idx}` |
| Master rpc_thread_num | `num_workers × 2` | `num_threads × 2` |
| global_segment_size 默认 | 4096 MB | 512 MB |
| core_bind_start 默认 | 5 | -1（禁用） |
| batch_get 预热 | 无（纯 Get 需先手动 Put） | 有 warmup 阶段（自动 BatchPut 所有 slot） |

**执行流程：**
1. **启动 Master**：同 nds_stress_test.py（`--use_od=true`）
2. **Phase I — 初始化 Store**：主进程 mmap 总 buffer（`num_threads × batch_size × block_size`），`MooncakeDistributedStore.setup()` + `register_buffer()`（触发 NDS init）
3. **Phase II — Spawn 线程**：每个线程拿到 buffer 的一个分片偏移
4. **Phase III — 压力测试**：线程循环执行 batch_put/batch_get/mixed
5. **停止 & 汇总**

**线程内部逻辑：**
```
1. 如果 batch_get/mixed 模式 → warmup：先 batch_put_from 所有 slot 的 key
2. 循环（直到 deadline）：
   a. 生成 batch_keys (t{thread}_s{slot}_k{idx})
   b. batch_put_from / batch_get_into / mixed
   c. eviction_window 淘汰旧 slot
   d. 通过 ThreadStats.add_batch_stats 上报
3. 清理：remove_by_regex alive slot
```

**命令行参数：**

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--operation-mode` | `batch_put` | `batch_put`/`batch_get`/`mixed` |
| `--block-size` | `128KB` | 单个 block 大小 |
| `--batch-size` | `128` | 每批 key 数量 |
| `--num-threads` | `8` | 工作线程数 |
| `--duration` | `30` | 测试时长（秒） |
| `--monitor-interval` | `1` | 监控间隔 |
| `--protocol` | `tcp` | 传输协议 |
| `--device-name` | `""` | RDMA 设备名 |
| `--local-hostname` | `127.0.0.1:0` | 本地 hostname |
| `--global-segment-size` | `512` | 全局 segment 大小（MB） |
| `--depth` | `1` | 环形缓冲深度 |
| `--eviction-window` | `1` | 最大存活 batch 数 |
| `--core-bind-start` | `-1` | CPU 绑核起始（-1 禁用） |
| `--master-binary` | `""` | mooncake_master 路径 |

**运行命令示例：**
```bash
# 默认 8 线程 batch_put
python nds_thread_stress_test.py

# 自定义参数
python nds_thread_stress_test.py \
  --num-threads=16 \
  --batch-size=256 \
  --block-size=65536 \
  --duration=60 \
  --depth=4 \
  --eviction-window=2

# Mixed 模式 + CPU 绑核
python nds_thread_stress_test.py \
  --operation-mode=mixed \
  --num-threads=8 \
  --core-bind-start=0

# RDMA
python nds_thread_stress_test.py \
  --protocol=rdma \
  --device-name=mlx5_0
```

**输出指标（与 nds_stress_test.py 类似，额外增加）：**
- 实时：区间带宽 + 累计带宽（两个维度）
- 最终报告同上

**核心设计特点：**
- **单进程共享 NDS**：验证 NDSLoader 全局单例在多线程并发 `batchPut/batchGet` 下的线程安全性
- **warmup 阶段**：batch_get 模式启动时自动 warmup Put，确保 Get 有数据可读
- **buffer 分片**：主进程一次 mmap，各线程拿偏移分片，减少 mmap 开销
- **ThreadStats 线程安全**：`threading.Lock` 保护统计累加，`snapshot_and_reset` 返回区间+累计双维度带宽

---

### 9.4 nds_data_correctness_test.py — Python 数据正确性验证

**定位：** `mooncake-store/tests/nds_data_correctness_test.py`，单进程单线程，专注于 NDS 数据读写正确性验证，不关心性能。

**核心设计：** 写入可验证的模式数据（`0-250` 循环 pattern），读取后逐字节校验。由于 NDS 注册在同一内存区域，Put 后源内存可能被覆盖，因此不使用内存对比——而是写入已知 pattern，清零源内存后读取，验证读回数据是否与 pattern 一致。

**数据模式：** `(np.arange(block_size, dtype=np.uint32) % 251).astype(np.uint8)` — 即 `0, 1, 2, ..., 250, 0, 1, 2, ...` 循环填充每个 block，长度由 `--block-size` 决定。

**测试阶段：**

| Phase | 测试内容 | 关键逻辑 |
|---|---|---|
| Phase 1 | 单键 Put → Get | 3 个 key 逐个 `put_from` → 清零源内存 → 逐个 `get_into` → 逐 block 验证 pattern |
| Phase 2 | BatchPut → BatchGet | `batch_put_from` N 个 key → 清零源内存 → `batch_get_into` → 逐 block 验证 pattern |
| Phase 3 | DISK replica 读取 | Put 数据（MEMORY + DISK 副本均存在） → Remove 全部 → 重新 Put → **清零整个 buffer** → `batch_get_into` → 验证读回数据是否正确（模拟 MEMORY 副本失效，强制走 DISK/NDS 路径） |
| Phase 4 | Remove + 验证对象已删除 | Remove 所有 key → `get_into` 已删除 key → 验证返回 length ≤ 0 |
| Phase 5 | RemoveByRegex | Put 3 个 `regex_test_*` key → `remove_by_regex("^regex_test_")` → 逐 key `get_into` → 验证均返回失败 |

**Phase 3 DISK 测试的关键设计：**
```
1. batch_put_from → 数据写入 NDS (DISK replica)
2. buf[:] = 0 → 清零整个 mmap buffer（包括注册的 segment 内存）
3. batch_get_into → 数据必须从 DISK replica 读取（内存已全零）
4. verify_buffer → 校验读回数据是否匹配 pattern
```
这模拟了 MEMORY 副本被淘汰或失效的场景，验证 NDS DISK 路径的数据完整性。

**命令行参数：**

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--block-size` | `4096` | 单个数据 block 大小（字节） |
| `--batch-size` | `4` | BatchPut/BatchGet 的 key 数量 |
| `--protocol` | `tcp` | 传输协议 |
| `--device-name` | `""` | RDMA 设备名 |
| `--local-hostname` | `127.0.0.1:0` | 本地 hostname |
| `--global-segment-size` | `64` | 全局 segment 大小（MB） |
| `--master-binary` | `""` | mooncake_master 路径 |

**运行命令示例：**
```bash
# 默认正确性测试
python nds_data_correctness_test.py

# 自定义 block/batch 大小
python nds_data_correctness_test.py \
  --block-size=65536 \
  --batch-size=8

# RDMA 模式
python nds_data_correctness_test.py \
  --protocol=rdma \
  --device-name=mlx5_0
```

**输出：** 逐 Phase PASS/FAIL 日志 + 最终汇总 "ALL TESTS PASSED" 或 "SOME TESTS FAILED"。进程退出码 0 = 全部通过，1 = 有失败。

**与压力测试脚本的核心区别：**

| 对比项 | nds_stress_test.py / nds_thread_stress_test.py | nds_data_correctness_test.py |
|---|---|---|
| 目标 | 压力/带宽/线程安全 | 数据读写正确性 |
| 并发 | 多进程/多线程 | 单线程 |
| 数据校验 | 无（只检查 retcode） | 逐字节 pattern 校验 |
| DISK 测试 | 无 | Phase 3 专门验证 DISK replica 读取 |
| Remove 测试 | 仅 eviction 窗口清理 | Phase 4+5 专门验证 Remove/RemoveByRegex |
| 持续时间 | 可配置 duration | 立即完成（秒级） |

---

## 十、构建变更

- 移除 `ndsclient` 库链接依赖（不再需要静态编译的 mock 库）
- NDS 通过 `dlopen("libndskv.so")` 动态加载，运行时依赖
- `NDS_LIBRARY_PATH` 环境变量可指定 so 路径
- CMake 新增 `kv_storage_backend.cpp` 源文件
- CMake 新增 `nds_client_test` 测试目标