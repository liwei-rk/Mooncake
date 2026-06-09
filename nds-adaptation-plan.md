# Mooncake 盘框（NDS）适配方案

## 概述

将 Mooncake Store 的磁盘持久化路径从传统文件系统（StorageBackend）替换为盘框 KV 存储（NDS），实现零拷贝磁盘读写，同时保持与非盘框部署的兼容性。核心机制：`use_od` 标志作为双后端路由开关，NDS 初始化从 `Client::Create` 延迟到 `RegisterLocalMemory` 触发，异步批量写入提升吞吐。nsid 从 Master 侧 gflag 配置，通过 `GetStorageConfig` RPC 自动下发到所有 Client。

---

## 一、新增文件

| 文件路径 | 说明 |
|---|---|
| `mooncake-store/include/kv_storage_backend.h` | KVStorageBackend 类声明 |
| `mooncake-store/src/kv_storage_backend.cpp` | 完整实现：NDSLoader（dlopen 动态加载 libndskv.so + nsid）、batch Put/Get 零拷贝、contiguous 校验 |
| `mooncake-store/tests/nds_client_test.cpp` | C++ GTest 单元测试：NDS Client 全流程 |
| `mooncake-store/tests/nds_stress_test.py` | 多进程压力测试 |
| `mooncake-store/tests/nds_thread_stress_test.py` | 多线程压力测试 |
| `mooncake-store/tests/nds_data_correctness_test.py` | 数据正确性验证（逐字节 pattern 校验） |
| `mooncake-store/tests/diagnose_network.py` | 网络诊断脚本 |

## 二、删除文件

| 文件路径 | 说明 |
|---|---|
| `mooncake-store/include/nds/nds_mock.cpp` | 旧 mock 实现，已被 KVStorageBackend + NDSLoader 替代 |
| `mooncake-store/include/nds/nds_mock_test_utils.cpp` | mock 测试辅助 |
| `mooncake-store/include/nds/nds_mock_test_utils.h` | mock 测试辅助头文件 |
| `mooncake-store/include/nds/test_nds_mock.cpp` | mock 单元测试 |
| `mooncake-store/src/nds_c_bridge.cpp` | 多余 C bridge，NDSLoader 直接 dlsym 加载 `extern "C"` 符号 |
| `mooncake-store/include/nds/nds_c_wrapper.cpp` | 同上 |

---

## 三、核心架构变更

### 3.1 NDS 初始化流程重构

kv-main 原流程：`Client::Create` → `PrepareStorageBackend(nds_mem_addr, nds_mem_size)` → `KVStorageBackend::Init` 立即初始化

kv_v6 新流程（两步）：

1. `Client::Create` → `GetStorageConfig` → `use_od_=true` → `PrepareStorageBackend` 只创建 KVStorageBackend（不调 Init）→ `setNsid(nsid_)`
2. `Client::RegisterLocalMemory(addr, length)` → 延迟触发 `kv_storage_backend_->Init(addr, length)` → NDSLoader::Load → dlopen → init

| 文件 | 改动 |
|---|---|
| `client_service.h` | 新增 `kv_storage_backend_`、`use_od_` bool、`HasDiskStorage()` |
| `client_service.cpp` | `PrepareStorageBackend` 不调 Init；`RegisterLocalMemory` 加 use_od_ NDS init；`unregisterLocalMemory` 加 CleanupNDS |
| `real_client.cpp` | 新增空 `ConfigDict` `{}`；`setup_real` 传入 `50052, false` |
| `dummy_client.h` | `setup_real` 签名移除 nds_mem 参数 |

### 3.2 NDS 释放流程

`Client::unregisterLocalMemory(addr)` → `kv_storage_backend_->CleanupNDS()` → free(owns_nds_memory_) + 重置 NDSLoader 全局状态 + `initialized_=false`

析构函数 `~KVStorageBackend()` 也调 `CleanupNDS()`。

### 3.3 use_od 双后端路由

| 操作 | use_od_=true | use_od_=false |
|---|---|---|
| 磁盘存储后端 | `KVStorageBackend` | `StorageBackend` |
| HasDiskStorage() | `kv_backend_ != nullptr` | `storage_backend_ != nullptr` |
| Put DISK replica | `StoreObjects` | `PutToLocalFile` |
| BatchPut DISK | 整批异步 StoreObjects + BatchPutEndDisk | 逐 key PutToLocalFile |
| Get DISK replica | `LoadObjects`（NDS FilereadTask） | `LoadObject`（文件 FilereadTask） |
| Remove | `kv_storage_backend_->Remove` | `storage_backend_->RemoveFile` |
| RemoveByRegex | `kv_storage_backend_->RemoveByRegex` | `storage_backend_->RemoveByRegex` |
| RemoveAll | `kv_storage_backend_->RemoveAll` | `storage_backend_->RemoveAll` |

### 3.4 Remove 后端路由（重新启用）

kv-main 中本地后端清理被注释掉（只删 master 元数据不删本地数据）。kv_v6 重新启用，按 `use_od_` 分支路由：

- `use_od_=true` → `kv_storage_backend_->Remove(key)` — NDS 不支持删除，空操作
- `use_od_=false` → `storage_backend_->RemoveFile(key)` — 文件系统本地删除

### 3.5 BatchPut NDS 异步化

kv-main：逐 key 同步 `PutToLocalFile` + 异步 `PutEnd(DISK)`

kv_v6 NDS 路径：
1. 收集所有 disk replica keys/slices
2. `write_thread_pool_.enqueue` → 整批 `StoreObjects(keys, slices)` → NDS `batchPut`
3. 成功 → `BatchPutEndDisk(keys)` RPC
4. 失败 → `PutRevoke(key, DISK)` per key
5. 每个 key 挂 `MemcpyOperationState` future 加入 `op.pending_transfers`
6. NDS 写入即发即弃 — MEMORY 成功 = Put 成功；DISK 失败仅撤销 DISK 副本

`WaitForTransfers` 仅等待 MEMORY TransferFutures。`FinalizeBatchPut` 为成功操作调用 `BatchPutEnd(MEMORY)`。

### 3.6 单键 Put NDS 路径

`Client::Put()` 新增 `use_od_` 分支：disk replica → `StoreObjects({key}, {slices})` → `BatchPutEndDisk({key})` 或 `PutRevoke(DISK)`。

### 3.7 TransferSubmitter 扩展

| 改动 | 说明 |
|---|---|
| 构造函数 | 新增 `std::shared_ptr<KVStorageBackend>& kv_backend`、`bool use_od` |
| FilereadWorkerPool | 新增 `kv_backend_`；worker 按 `task.use_nds` 分流到 NDS 或文件 |
| FilereadTask | 新增 `use_nds` bool + `nds_key` string |
| submitBatchMemcpyOperation | 新增：批量 memcpy |
| submitBatchFileReadOperation | 新增：批量 NDS 读取 |
| BatchFilereadTask | 新增 struct：`nds_keys` + `batched_slices` + `state` |
| FilereadWorkerPool 队列 | 改为 `variant<FilereadTask, BatchFilereadTask>` |
| MemcpyWorkerPool | 1→4 threads（`MC_MEMCPY_WORKERS` 可配置 1~64） |
| TransferFuture | 不可拷贝→可拷贝（支持 batch future 管理） |

### 3.8 BatchGet NDS 读取路径

**跨客户端 NDS 读取完整数据流：**

Client B `BatchGet` → Master 返回 DISK 副本 descriptor → local_buffer allocate（slices 指向 local_buffer 区域）→ TransferSubmitter 路由到 `submitBatchFileReadOperation` → `BatchFilereadTask` → `FilereadWorkerPool` → `kv_backend_->LoadObjects(nds_keys, slices)` → NDS C API `c_batchGet` 直接读入 slices 的 ptr → 数据在 local_buffer 中，**完全绕过 global_segment**

MEMORY 副本被驱逐后：无"重新加热"机制 — 数据永远留在 NDS，每次 Get 都走 NDS 直接读入 local_buffer。

### 3.9 Master 端变更

**master_config.h：** 所有 Config 类新增 `bool use_od` + `uint32_t nsid`

**master_service.h/cpp：**
- 新增 `const uint32_t nsid_` 成员
- `use_od_=true && nsid_==0` → 禁用 DISK replica + LOG(WARNING)（静默降级）
- `use_od_=true && nsid_>0` → `use_disk_replica_=true`
- `PutStart` DISK replica：`root_fs_dir_` 空时 `file_path=""`
- 新增 `BatchPutEndDisk` 方法
- `BatchPutRevoke` 同时撤销 MEMORY + DISK（任一成功即返回成功）
- `GetStorageConfig` 返回新增 `use_od_` + `nsid_`

**rpc_service.h/cpp：** 新增 `BatchPutEndDisk` RPC handler

**rpc_types.h：** `GetStorageConfigResponse` 新增 `bool use_od` + `uint32_t nsid`

**master_client.h/cpp：** 新增 `BatchPutEndDisk(keys)` 方法

**master.cpp：** 新增 `DEFINE_uint32(nsid, 0)` gflag；启动校验 `use_od=true && nsid==0` 静默降级

**client_service.h/cpp：** 新增 `uint32_t nsid_{0}`；两个 `GetStorageConfig` 接收点设置 `nsid_`；`PrepareStorageBackend` 改为 `setNsid(nsid_)`

### 3.10 Python binding 变更

`store_py.cpp`：移除 `nds_mem_addr/nds_mem_size` 参数；新增 glog 控制绑定（`init_glog`、`set_vlog_level`、`set_log_to_stderr`）

### 3.11 NDS 接口重构

**nds_interface.h：** 新增 `extern "C"` 声明 + `batchGet/batchPut` + nsid 参数

**NDSLoader typedefs：**
```cpp
typedef int32_t (*NDS_init_fn)(void*, uint64_t, const char*);
typedef int32_t (*NDS_get_fn)(uint64_t, uint8_t*, size_t, size_t, uint32_t);
typedef int32_t (*NDS_put_fn)(uint64_t, uint8_t*, size_t, size_t, uint32_t);
typedef int32_t (*NDS_batchGet_fn)(const uint64_t*, uint8_t**, const size_t*, const size_t*, const uint32_t*, uint32_t);
typedef int32_t (*NDS_batchPut_fn)(const uint64_t*, uint8_t**, const size_t*, const size_t*, const uint32_t*, uint32_t);
```

**NDSLoader::Load()：** `MC_NDS_CONFIG` 环境变量（默认 `nds_config.conf`）；所有 `dlsym` 改用 `c_*` C-linkage 符号名

**nsid 传递数据流：**
```
master --nsid=1 → MasterConfig.nsid → MasterService.nsid_
  → GetStorageConfigResponse.nsid → Client.nsid_
    → kv_storage_backend_->setNsid(nsid_)
      → StoreObjects/LoadObjects 内部自动填充 nsids(count, nsid_)
        → NDSLoader::batchPut/batchGet(..., nsids, count)
```

`MC_NDS_NSID` 环境变量已完全移除。

### 3.12 CMake 变更

- 新增 `kv_storage_backend.cpp` 到源文件列表
- 移除 `ndsclient` 库链接
- 新增 `nds_client_test` 测试目标

---

## 四、StoreObjects/LoadObjects 零拷贝设计

NDS 零拷贝要求源数据地址在 NDS init 内存区域内。

**StoreObjects：** 每个 key 的 slices 做 contiguous 校验（相邻 slice ptr+size == 下一个 ptr）→ contiguous 合并为单个 NDS entry（blockId, 首slice.ptr, offset=0, total_size）→ 不 contiguous 返回 `INVALID_PARAMS` → `NDSLoader::batchPut(blockIds, addrs, offsets, lengths, nsids, count)`

**LoadObjects：** 同理，contiguous 校验 + `NDSLoader::batchGet` 直接写入 slices ptr（即 local_buffer 区域）

**objectKeyToUint64：** 字符串 key 转 uint64_t blockId

---

## 五、KVStorageBackend 类结构

```cpp
class KVStorageBackend {
public:
    Init(void* nds_mem_addr = nullptr, uint64_t nds_mem_size = 0);
    CleanupNDS();
    isInitialized() const;  // atomic
    StoreObjects(keys, batched_slices);
    LoadObjects(keys, batched_slices);
    Remove(key);            // NDS 不支持，空操作
    RemoveByRegex(key);     // NDS 不支持
    RemoveAll();            // NDS 不支持
    void setNsid(uint32_t nsid);
    uint32_t nsid() const;

private:
    bool owns_nds_memory_{false};
    void* nds_mem_addr_ = nullptr;
    uint64_t nds_mem_size_ = 0;
    std::atomic<bool> initialized_{false};
    uint32_t nsid_{0};
};
```

**NDSLoader（进程级全局单例）：** `dlopen("libndskv.so")` → `dlsym c_init/c_get/c_put/c_batchGet/c_batchPut/c_isExists`。NDS C API 无 context/handle 参数，不支持多实例。Init 时：有外部内存 → 用用户地址；无外部内存 → `aligned_alloc(4096, 1GB)` 自分配。

---

## 六、关键设计决策

| 决策 | 原因 |
|---|---|
| NDS init 由 RegisterLocalMemory 触发 | 零拷贝要求源数据在 NDS 内存区域内；register_buffer 时才知道可用内存 |
| NDSLoader 全局单例 | NDS C API 全局状态，不支持多实例 |
| nsid 存在 KVStorageBackend | NDSLoader 全局单例无法存 per-client 状态；KVStorageBackend 与 Client 1:1 |
| nsid 由 Master RPC 下发 | 部署级配置，不暴露给上层 API 或环境变量 |
| use_od=true + nsid=0 静默降级 | 不 exit/fatal，自动禁用 DISK replica + LOG(WARNING) |
| NDS 写入即发即弃 | MEMORY 成功 = Put 成功；DISK 失败仅撤销 DISK 副本，不阻塞 WaitForTransfers |
| BatchPut 整批异步提交 | 减少 RPC 调用，StoreObjects 一次处理所有 disk key |
| TransferFuture 可拷贝 | NDS batch 写入需将 MemcpyOperationState future 加入 pending_transfers |
| 跨客户端 NDS 读取绕过 global_segment | NDS `c_batchGet` 直接写入 local_buffer slices ptr，无需中间缓冲 |
| LOCAL_DISK 副本全部拒绝 | NDS 模式不启用 offload，LOCAL_DISK 不会出现；防御性兜底避免 crash |
| 副本优先级 MEMORY > DISK(NDS) > LOCAL_DISK(跳过) | FindFirstCompleteReplica 优先选 MEMORY，跳过 LOCAL_DISK |

---

## 七、NDS 初始化流程

```
Python: store.setup(use_od=True)
  → Client::Create
    → GetStorageConfig → use_od_=true, nsid_=1
    → PrepareStorageBackend → 创建 KVStorageBackend（不调 Init）→ setNsid(nsid_)
  → Python: store.register_buffer(ptr, size)
    → Client::RegisterLocalMemory(ptr, size)
      → use_od_=true && kv_storage_backend_ && !isInitialized()
        → KVStorageBackend::Init(ptr, size)
          → NDSLoader::Load()
            → dlopen("libndskv.so")
            → dlsym c_init/c_get/c_put/c_batchGet/c_batchPut/c_isExists
            → MC_NDS_CONFIG env → nds_config_path（默认 nds_config.conf）
          → NDSLoader::init(ptr, size, nds_config_path.c_str()) → c_init
          → initialized_ = true
```

## 八、NDS 释放流程

```
Python: store.unregister_buffer(ptr)
  → Client::unregisterLocalMemory(ptr)
    → use_od_=true && kv_storage_backend_ && isInitialized()
      → KVStorageBackend::CleanupNDS()
        → if owns_nds_memory_ → free(nds_mem_addr_)
        → 重置 NDSLoader 全局状态
        → initialized_ = false
```

## 九、NDS BatchPut 流程

```
Python: store.batch_put(keys, values)
  → StartBatchPut → BatchPutStart RPC → 分配 replicas

  → SubmitTransfers
    → NDS 路径（use_od_=true，所有 DISK replica key）
      → 收集所有 disk key + slices
      → write_thread_pool_.enqueue:
          → StoreObjects(keys, slices) → NDS batchPut
          → 成功 → BatchPutEndDisk(keys) RPC
          → 失败 → PutRevoke(key, DISK) per key
      → 每个 key 挂 MemcpyOperationState future

    → Memory 路径（所有 MEMORY replica ops）
      → TransferWrite → TransferEngine batch submit

  → WaitForTransfers → 等待 NDS future + Memory future

  → FinalizeBatchPut
    → 成功 → BatchPutEnd(MEMORY)
    → 失败 → BatchPutRevoke(MEMORY+DISK, 任一成功即返回成功)
```

## 十、NDS BatchGet 流程

```
Python: store.batch_get(keys)
  → QueryBatch → BatchQuery RPC → 返回 replica 列表

  → BatchGet (use_od_=true)
    → 逐 key 查找 replica
      → FindFirstCompleteReplica → 跳过 LOCAL_DISK → 找到 MEMORY 或 DISK
      → MEMORY → 放入 memory_replicas / memory_slices / memory_indices
      → DISK → 放入 disk_replicas / disk_slices / disk_indices

    → 同时提交两组
      → mem_future = submit_batch(memory_replicas, memory_slices, READ)
        → LOCAL_MEMCPY 或 TRANSFER_ENGINE
      → disk_future = submit_batch(disk_replicas, disk_slices, READ)
        → submitBatchFileReadOperation → BatchFilereadTask → LoadObjects → NDS c_batchGet

    → 分别等待
      → mem_result = mem_future->get()
      → disk_result = disk_future->get()

    → 检查 lease 过期 → return results

  → BatchGet (use_od_=false)
    → 逐 key: submit(replica, slices) → per-key future.get()
```

## 十一、nsid 配置数据流

```
master --nsid=1 → FLAGS_nsid=1 → InitMasterConf → master_config.nsid=1
  → MasterService(config)
    → nsid_(config.nsid)
    → GetStorageConfigResponse.nsid → Client.nsid_
      → PrepareStorageBackend → kv_storage_backend_->setNsid(nsid_)
        → StoreObjects/LoadObjects 内部 nsids(count, nsid_)
```

---

## 十二、BatchGet 类型分组与 LOCAL_DISK 拒绝

原始 `BatchGet(use_od_)` 混装所有 replica 类型到 batch，但 `submit_batch` 只根据 `replicas[0]` 路径。混合类型会导致 crash。

**修复：**
1. `FindFirstCompleteReplica` 跳过 LOCAL_DISK — 只返回 MEMORY 或 DISK
2. `BatchGet(use_od_)` 按副本类型分组提交：MEMORY 组走 RDMA/TCP/memcpy，DISK 组走 NDS
3. `TransferRead()` 增加 LOCAL_DISK 拒绝：`else if (is_disk_replica())` 取 object_size，其余返回 `INVALID_REPLICA`
4. `TransferSubmitter::submit()` `else` → `else if (is_disk_replica())`，LOCAL_DISK 返回 `std::nullopt`
5. `TransferSubmitter::submit_batch()` 前置遍历拒绝 LOCAL_DISK

| 文件 | 修改 |
|---|---|
| `client_service.h:581-583` | FindFirstCompleteReplica 跳过 LOCAL_DISK |
| `client_service.cpp:2692` | 实现体：`!is_local_disk_replica()` 过滤 |
| `client_service.cpp:877-976` | BatchGet(use_od_) 整段重写：分组 + 双 future |
| `client_service.cpp:2396-2405` | TransferRead LOCAL_DISK 拒绝 |
| `transfer_task.cpp:540-545` | submit() LOCAL_DISK 拒绝 |
| `transfer_task.cpp:559-566` | submit_batch() 前置 LOCAL_DISK 遍历拒绝 |

---

## 十三、测试

### 13.1 nds_client_test.cpp — C++ GTest

537 行。`SetUpTestSuite`：InProcMaster(use_od=true, nsid=1) → Client → Segment Provider → 256MB buffer → RegisterLocalMemory（触发 NDS init）

运行：`cd build && MC_METADATA_SERVER=http://127.0.0.1:8080/metadata ./mooncake-store/tests/nds_client_test --protocol=tcp`

| 测试名 | 逻辑 | 数据 |
|---|---|---|
| PutAndGetSingleKey | 单 key Put→Get→memcmp | 1MB |
| BatchPutAndBatchGet | 10 key BatchPut→BatchGet→memcmp | 10×1MB |
| BatchPutWithMultiSlicePerKey | 3 key + 2 slice each | 3×(1+2)MB |
| OverwriteExistingKey | Put→Remove→Put→Get | 1MB→2MB |
| LargePayloadPutAndGet | 16MB | 16MB |

### 13.2 nds_stress_test.py — 多进程压力测试

643 行。每个 Worker 独立进程 + 独立 NDS 实例。Master：`--use_od=true --nsid=1`

```bash
python nds_stress_test.py --num-workers=4 --duration=60 --protocol=tcp
```

### 13.3 nds_thread_stress_test.py — 多线程压力测试

598 行。单进程内所有线程共享一个 NDS 实例，验证 NDSLoader 全局单例多线程安全性。

```bash
python nds_thread_stress_test.py --num-threads=16 --duration=60
```

### 13.4 nds_data_correctness_test.py — 数据正确性验证

单进程单线程，逐字节 pattern 校验。Phase 3 专门验证 DISK replica 读取：Put→清零 buffer→Get→校验 NDS 读取正确性。

```bash
python nds_data_correctness_test.py --block-size=65536 --batch-size=8
```

---

## 十四、构建与运行依赖

- NDS 通过 `dlopen("libndskv.so")` 动态加载，运行时依赖
- `NDS_LIBRARY_PATH` 环境变量指定 so 路径，默认 `libndskv.so`
- `MC_NDS_CONFIG` 指定 NDS 配置文件路径，默认 `nds_config.conf`
- `dlsym` 使用 `c_*` C-linkage 符号名
- `MC_NDS_NSID` 已移除，nsid 通过 Master `--nsid` gflag 配置
- `c_init` 新增第三参数 `const char* path_nds_config`

---

## 十五、关键差异速查（kv-main vs kv_v6）

| kv-main | kv_v6 |
|---|---|
| DISK → 共享文件系统(3FS/NFS) | DISK → NDS KV 存储(零拷贝) |
| file_path = root_fs_dir/... | file_path = ""（NDS 不需要） |
| 磁盘写入 = 文件异步写入(1拷贝) | 磁盘写入 = NDS batchPut(0拷贝) |
| 磁盘读取 = preadv(0拷贝) | 磁盘读取 = NDS batchGet(0拷贝，绕过 global_segment) |
| BatchPutRevoke 只撤销 MEMORY | BatchPutRevoke 撤销 MEMORY+DISK |
| 本地清理被注释掉 | 重新启用(use_od_ 双路由) |
| GetStorageConfig 返回 3 字段 | 返回 5 字段(+use_od, +nsid) |
| 无 use_od/nsid 配置 | Master gflag(--use_od, --nsid) |
| NDS init 在 Create 时 | NDS init 延迟到 RegisterLocalMemory |
| nsid = MC_NDS_NSID 环境变量 | nsid = Master RPC 下发 |
| use_disk_replica ← MountDiskSegment | use_disk_replica ← use_od && nsid>0 |
| root_fs_dir 必须非空 | root_fs_dir 可为空 |
| StorageBackend(FilePerKey等) | KVStorageBackend(NDSLoader) |
| FilereadTask(单文件) | BatchFilereadTask(批量NDS) |
| TransferFuture 不可拷贝 | TransferFuture 可拷贝 |
| MemcpyWorker 1 thread | MemcpyWorker 4 threads |
| 同节点默认走 RDMA | 同节点默认走 RDMA；MC_STORE_MEMCPY=1 开启 memcpy |

---

## 十六、待办与已知问题

### 已完成
- 诊断元数据超时、local_buffer_size 根因，选项 A 修复
- 清理调试 LOG(INFO)
- 修复 4 个 Python NDS 测试脚本
- NSID 主配置实现（8 个 C++ 文件 + 4 个 Python 测试）
- NDS C API 接口变更（dlsym 迁移到 c_* 符号）
- 批量副本类型处理 bug 修复（LOCAL_DISK 跳过）
- 从 SubmitTransfers 移除 nds_states — NDS 写入即发即弃
- BatchGet 按副本类型分组提交

### 待修复
- **`client_service.cpp` ~1447 行重复 `has_disk_replica` 声明** — 编辑 bug

### 待实现
- **重构 BatchGet：将副本类型拆分从 Client 移至 TransferSubmitter**
  - CompositeOperationState：逐索引 ErrorCode 数组
  - TransferStrategy 添加 COMPOSITE 枚举值
  - TransferFuture 添加 `get_index_results()` 方法
  - TransferSubmitter::submit_batch 内部拆分混合类型副本
  - 简化 Client::BatchGet use_od_ 路径

### 阻塞
- 环境中无 cmake — 无法验证编译