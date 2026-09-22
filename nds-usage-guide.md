# Mooncake NDS（盘框 KV 存储）使用指南

## 1. 架构概述

NDS 版 Mooncake 将磁盘持久化路径从传统文件系统替换为盘框 KV 存储（NDS），实现零拷贝的磁盘读写。通过 `use_od` 标志作为双后端路由开关：

- `use_od=true` → 使用 `KVStorageBackend`（NDS 盘框存储），数据通过 `batchPut/batchGet` C API 零拷贝读写
- `use_od=false` → 使用 `StorageBackend`（传统文件系统），数据通过文件异步写入 / preadv 读写

**关键特性：**
- NDS 初始化延迟到 `register_buffer()` 触发（复用用户提供的内存区域，零拷贝要求源数据在 NDS 内存区域内）
- nsid（NDS namespace ID）由 Master 侧配置，通过 RPC 自动下发到所有 Client，无需在客户端设置环境变量
- NDS 通过 `dlopen("libndskv.so")` 动态加载，无需编译时链接
- `use_od=true + nsid=0` 时静默降级：自动禁用 DISK replica，LOG(WARNING)，不 exit

---

## 2. 系统要求

| 组件 | 要求 |
|---|---|
| **NDS 共享库** | `libndskv.so` — NDS 盘框 KV 存储的动态链接库 |
| **NDS 配置文件** | `nds_config.conf` — NDS 初始化配置（默认文件名） |
| **Metadata Server** | HTTP metadata server 或 Etcd |
| **传输协议** | TCP 或 RDMA |

---

## 3. Master 命令行配置

### 3.1 基本启动命令

```bash
mooncake_master \
    --use_od=true \
    --nsid=1 \
    --enable_http_metadata_server=true \
    --rpc_address=0.0.0.0 \
    --rpc_port=50051 \
    --http_metadata_server_host=0.0.0.0 \
    --http_metadata_server_port=8080 \
    --cluster_id=your_cluster
```

### 3.2 NDS 相关 gflags

| Flag | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `--use_od` | bool | `false` | 启用 NDS（KV）存储后端。`true` = 盘框模式，`false` = 传统文件系统 |
| `--nsid` | uint32 | `0` | NDS namespace ID。**use_od=true 时必须 > 0**，否则静默降级为 use_od=false |

**注意：** `use_od=true + nsid=0` 会触发静默降级：
- Master 启动时：LOG(WARNING) + 自动设置 `use_od=false`
- MasterService 构造时：禁用 `use_disk_replica_` + LOG(WARNING)
- 不影响其他功能正常运行

### 3.3 常用 Master 配置 flags

| Flag | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `--rpc_port` | int32 | `50051` | RPC 服务端口 |
| `--rpc_address` | string | `0.0.0.0` | RPC 绑定地址 |
| `--rpc_thread_num` | int32 | `0` | RPC 线程数（0 = 使用 max_threads） |
| `--enable_http_metadata_server` | bool | `false` | 启用 HTTP metadata server |
| `--http_metadata_server_port` | int32 | `8080` | HTTP metadata 端口 |
| `--http_metadata_server_host` | string | `0.0.0.0` | HTTP metadata 绑定地址 |
| `--cluster_id` | string | `default_cluster` | 集群 ID |
| `--root_fs_dir` | string | （空） | 文件系统根目录。**NDS 模式可为空** |
| `--enable_disk_eviction` | bool | `true` | 启用磁盘淘汰 |
| `--quota_bytes` | uint64 | `0` | 存储配额（0 = 使用默认 90% 容量） |
| `--default_kv_lease_ttl` | string | `30000` | KV 对象默认租约 TTL（ms 或带 s/m/h 后缀） |
| `--config_path` | string | （空） | 配置文件路径（见第 5 节） |

---

## 4. 客户端环境变量

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `MC_METADATA_SERVER` | **必设** | Metadata server 地址，如 `http://127.0.0.1:8080/metadata` |
| `NDS_LIBRARY_PATH` | `libndskv.so` | NDS 共享库路径（绝对路径或相对路径） |
| `MC_NDS_CONFIG` | `nds_config.conf` | NDS 配置文件路径（含文件名，默认当前执行目录下） |
| `MC_MEMCPY_WORKERS` | `4` | MemcpyWorkerPool 线程数（1~64） |

**注意：** `MC_NDS_NSID` 环境变量已完全移除，nsid 由 Master RPC 自动下发。

---

## 5. 配置文件配置法

Master 支持通过 `--config_path` 指定配置文件，配置文件中的 key 会覆盖默认值，命令行 flags 可进一步覆盖配置文件。

### 5.1 Master 配置文件

创建配置文件（如 `master.conf`），格式为 `key = value`：

```ini
# NDS 核心配置
use_od = true
nsid = 1

# RPC 服务配置
rpc_port = 50051
rpc_address = 0.0.0.0
rpc_thread_num = 4

# HTTP Metadata Server
enable_http_metadata_server = true
http_metadata_server_port = 8080
http_metadata_server_host = 0.0.0.0

# 集群配置
cluster_id = my_nds_cluster
root_fs_dir =                  # NDS 模式可为空
enable_disk_eviction = true
quota_bytes = 0

# 租约配置（ms 或带 s/m/h 后缀）
default_kv_lease_ttl = 500
default_kv_soft_pin_ttl = 30000

# 淘汰配置
eviction_ratio = 0.5
eviction_high_watermark_ratio = 0.8

# 内存分配
memory_allocator = offset
allocation_strategy = random
global_file_segment_size = 17179869184    # 16GB
```

启动命令：

```bash
mooncake_master --config_path=master.conf
```

配置优先级：**命令行 flag > 配置文件 > 默认值**

例如配置文件设置 `nsid=1`，但命令行 `--nsid=2`，最终 nsid=2。

### 5.2 NDS 配置文件

NDS 配置文件由 `MC_NDS_CONFIG` 环境变量指定路径（默认当前执行目录的 `nds_config.conf`）。该文件在 NDS `c_init()` 初始化时传入，具体格式由 NDS 盘框库实现定义。

设置方式：

```bash
# 方式一：环境变量（推荐）
export MC_NDS_CONFIG=/path/to/nds_config.conf

# 方式二：默认路径（将文件放在当前执行目录下，命名为 nds_config.conf）
cp your_nds_config nds_config.conf
```

---

## 6. Python API

### 6.1 导入与初始化

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
```

### 6.2 setup() — 连接 Master

**方式一：参数式（推荐）**

```python
retcode = store.setup(
    local_hostname="127.0.0.1:0",      # 本地 hostname（端口 0 自动分配）
    metadata_server="http://127.0.0.1:8080/metadata",  # metadata server 地址
    global_segment_size=16 * 1024 * 1024,  # 全局 segment 大小（默认 16MB）
    local_buffer_size=0,                # NDS 模式设为 0（重要！）
    protocol="tcp",                     # 传输协议：tcp / rdma
    rdma_devices="",                    # RDMA 设备名（TCP 模式为空）
    master_server_addr="127.0.0.1:50051",  # Master RPC 地址
)
```

**方式二：字典式**

```python
retcode = store.setup({
    "local_hostname": "127.0.0.1:0",
    "metadata_server": "http://127.0.0.1:8080/metadata",
    "global_segment_size": str(16 * 1024 * 1024),
    "local_buffer_size": "0",
    "protocol": "tcp",
    "master_server_addr": "127.0.0.1:50051",
})
```

**关键：** NDS 模式下 `local_buffer_size` 应设为 `0`，否则首次 `register_buffer()` 触发 NDS init 时内存过小导致失败。

**返回值：** `0` = 成功，非 0 = 错误码。

### 6.3 register_buffer() — 注册内存并触发 NDS 初始化

```python
import mmap

# 分配内存
buffer_size = 1024 * 1024 * 1024  # 1GB
mm = mmap.mmap(-1, buffer_size)

# 获取内存地址
buf_ptr = ctypes.cast(ctypes.c_char_p(mm), ctypes.c_void_p).value or \
          int.from_bytes(mm.__buffer_info__()[0].to_bytes(8, 'little'), 'little')

# 注册内存 → 首次调用自动触发 NDS init
retcode = store.register_buffer(buf_ptr, buffer_size)
```

**重要：** 首次 `register_buffer()` 会触发 NDS 初始化：
- NDS `c_init(memAddr, length, nds_config_path)` 使用注册的内存地址和大小
- NDSLoader 加载 `libndskv.so`（路径由 `NDS_LIBRARY_PATH` 环境变量指定）
- 读取 `MC_NDS_CONFIG` 环境变量获取 NDS 配置文件路径
- NDS 内存必须足够容纳零拷贝读写的数据

### 6.4 数据写入

#### 单键写入 — put()

```python
retcode = store.put("my_key", b"my_value_data")
```

#### 单键零拷贝写入 — put_from()

```python
# 直接从已分配的 buffer 地址写入（零拷贝，数据必须在 NDS 内存区域内）
retcode = store.put_from("my_key", buf_ptr + offset, data_length)
```

#### 批量写入 — batch_put_from()

```python
keys = ["key1", "key2", "key3"]
buffer_ptrs = [buf_ptr + 0, buf_ptr + 1024, buf_ptr + 2048]
sizes = [1024, 1024, 1024]

ret_codes = store.batch_put_from(keys, buffer_ptrs, sizes)
# ret_codes: list of int，每个 key 的返回码
```

### 6.5 数据读取

#### 单键读取 — get()

```python
value = store.get("my_key")
# 返回 bytes 对象，失败时返回空 bytes
```

#### 单键零拷贝读取 — get_into()

```python
# 直接读入已分配的 buffer（零拷贝）
length = store.get_into("my_key", buf_ptr + offset, max_length)
# 返回实际读取的字节数，失败时返回 -1
```

#### 批量读取 — batch_get_into()

```python
keys = ["key1", "key2", "key3"]
buffer_ptrs = [buf_ptr + 0, buf_ptr + 1024, buf_ptr + 2048]
sizes = [1024, 1024, 1024]

ret_codes = store.batch_get_into(keys, buffer_ptrs, sizes)
# ret_codes: list of int，每个 key 的返回码
```

### 6.6 删除操作

```python
# 单键删除
store.remove("my_key", force=True)

# 按正则批量删除
removed = store.remove_by_regex("^temp_", force=True)

# 删除所有对象
store.remove_all(force=True)
```

**注意：** NDS 模式下 `remove/remove_by_regex/remove_all` 调用 NDS C API 删除对象；NDS 不支持删除时会为空操作。

### 6.7 释放资源

```python
# 注销内存 → 触发 NDS Cleanup
store.unregister_buffer(buf_ptr)
```

### 6.8 日志控制

```python
from mooncake.store import init_glog, set_vlog_level, set_log_to_stderr

init_glog("my_app")           # 初始化 glog
set_vlog_level(1)             # 设置 VLOG 级别
set_log_to_stderr(True)       # 日志输出到 stderr
```

---

## 7. 完整使用示例

### 7.1 最小可运行示例

```python
import ctypes
import mmap
from mooncake.store import MooncakeDistributedStore

# 1. 创建 Store
store = MooncakeDistributedStore()

# 2. 连接 Master（NDS 模式 local_buffer_size=0）
retcode = store.setup(
    local_hostname="127.0.0.1:0",
    metadata_server="http://127.0.0.1:8080/metadata",
    local_buffer_size=0,
    protocol="tcp",
    master_server_addr="127.0.0.1:50051",
)
assert retcode == 0, f"setup failed: {retcode}"

# 3. 分配并注册内存（触发 NDS init）
buffer_size = 256 * 1024 * 1024  # 256MB
mm = mmap.mmap(-1, buffer_size)
buf_ptr = ctypes.cast(ctypes.c_char_p(mm), ctypes.c_void_p).value
retcode = store.register_buffer(buf_ptr, buffer_size)
assert retcode == 0, f"register_buffer failed: {retcode}"

# 4. 写入数据
data = b"hello nds world"
retcode = store.put("test_key", data)
assert retcode == 0, f"put failed: {retcode}"

# 5. 读取数据
value = store.get("test_key")
assert value == data, f"data mismatch"

# 6. 清理
store.remove("test_key", force=True)
store.unregister_buffer(buf_ptr)
mm.close()
```

### 7.2 批量零拷贝读写示例

```python
import ctypes
import mmap
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
retcode = store.setup(
    local_hostname="127.0.0.1:0",
    metadata_server="http://127.0.0.1:8080/metadata",
    local_buffer_size=0,
    protocol="tcp",
    master_server_addr="127.0.0.1:50051",
)

# 分配大内存
buffer_size = 512 * 1024 * 1024  # 512MB
mm = mmap.mmap(-1, buffer_size)
buf_ptr = ctypes.cast(ctypes.c_char_p(mm), ctypes.c_void_p).value
store.register_buffer(buf_ptr, buffer_size)

# 准备批量数据
block_size = 1024 * 1024  # 1MB per block
batch_size = 10
keys = [f"block_{i}" for i in range(batch_size)]
ptrs = [buf_ptr + i * block_size for i in range(batch_size)]
sizes = [block_size] * batch_size

# 填充测试数据到 buffer
for i in range(batch_size):
    mm[i * block_size : (i + 1) * block_size] = bytes([i & 0xFF]) * block_size

# 批量写入
ret_codes = store.batch_put_from(keys, ptrs, sizes)

# 批量读取
ret_codes = store.batch_get_into(keys, ptrs, sizes)

# 验证数据
for i in range(batch_size):
    assert mm[i * block_size] == bytes([i & 0xFF])[0]

# 清理
store.remove_by_regex("^block_", force=True)
store.unregister_buffer(buf_ptr)
mm.close()
```

---

## 8. C++ API

### 8.1 Client 创建与内存注册

```cpp
#include "client_service.h"

auto client = Client::Create(
    "127.0.0.1:0",                           // local_hostname
    "P2PHANDSHAKE",                           // session_name
    "tcp",                                    // protocol
    std::nullopt,                             // rdma_devices
    "127.0.0.1:50051",                        // master_address (rpc)
    nullptr,                                  // transfer_engine
    {}                                        // config_dict（空字典）
);

// 注册内存 → 触发 NDS init
void* buffer = aligned_alloc(4096, 256 * 1024 * 1024);
client->RegisterLocalMemory(buffer, 256 * 1024 * 1024, "cpu:0");
```

### 8.2 数据读写

```cpp
// 单键 Put
client->Put(key, slices, ReplicateConfig{});

// 单键 Get
client->Get(key, slices);

// 批量 Put
client->BatchPut(keys, value_slices_list, ReplicateConfig{});

// 批量 Get
client->BatchGet(keys, value_slices_list);
```

---

## 9. nsid 配置数据流

nsid 由 Master 侧统一管理，自动下发，无需客户端手动配置：

```
Master 启动：
  mooncake_master --use_od=true --nsid=1
    → MasterConfig.nsid = 1
      → MasterService.nsid_ = 1
        → GetStorageConfig RPC → response.nsid = 1

Client 接收：
  Client::Create
    → GetStorageConfig → client->nsid_ = 1
      → PrepareStorageBackend → kv_storage_backend_->setNsid(1)

NDS 调用：
  KVStorageBackend::StoreObjects / LoadObjects
    → 自动填充 nsids(count, nsid_) 到每个 NDS batchPut/batchGet 调用
```

---

## 10. NDS C API 符号说明

`libndskv.so` 导出以下 `extern "C"` 符号，Mooncake 通过 `dlopen/dlsym` 动态加载：

| 符号名 | 签名 | 说明 |
|---|---|---|
| `c_init` | `int32_t c_init(void *memAddr, uint64_t length, const char *path_nds_config)` | 初始化 NDS，传入内存区域和配置文件路径 |
| `c_isExists` | `int32_t c_isExists(const uint64_t *blockIds, size_t count)` | 检查 block 是否存在 |
| `c_get` | `int32_t c_get(uint64_t blockId, uint8_t *addr, size_t offset, size_t length, uint32_t nsid)` | 读取单个 block |
| `c_put` | `int32_t c_put(uint64_t blockId, uint8_t *addr, size_t offset, size_t length, uint32_t nsid)` | 写入单个 block |
| `c_batchGet` | `int32_t c_batchGet(const uint64_t *blockIds, uint8_t **addrs, const size_t *offsets, const size_t *lengths, const uint32_t *nsids, size_t count)` | 批量读取 |
| `c_batchPut` | `int32_t c_batchPut(const uint64_t *blockIds, uint8_t **addrs, const size_t *offsets, const size_t *lengths, const uint32_t *nsids, size_t count)` | 批量写入 |

---

## 11. 运行测试

### 11.1 C++ 单元测试

```bash
# 先启动 HTTP metadata server
mooncake_http_metadata_server --port 8080 &

# 运行 NDS client 测试
cd build
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
  ./mooncake-store/tests/nds_client_test --protocol=tcp
```

### 11.2 Python 测试

```bash
# 先启动 HTTP metadata server
mooncake_http_metadata_server --port 8080 &

# 运行 Python 测试（自动启动 Master）
cd mooncake-store/tests
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
  python nds_data_correctness_test.py --protocol=tcp
```

### 11.3 Python 压力测试

```bash
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
  python nds_stress_test.py --operation-mode=mixed --num-workers=4 --duration=60 --protocol=tcp
```

---

## 12. 常见问题

### Q: `use_od=true + nsid=0` 会怎样？

静默降级：Master 自动设置 `use_od=false`，禁用 DISK replica，LOG(WARNING)。不影响 MEMORY replica 正常工作。

### Q: `local_buffer_size` 应设为多少？

NDS 模式下设为 **0**。原因：首次 `register_buffer()` 触发 NDS init 时，如果 `local_buffer_size > 0` 会使用过小的内存区域（28KB），导致 NDS 初始化失败。设为 0 时，NDS init 使用 `register_buffer` 提供的完整内存区域。

### Q: NDS 内存区域大小如何决定？

由 `register_buffer()` 传入的内存大小决定。NDS `c_init(memAddr, length, config_path)` 使用该内存区域。建议至少 256MB，推荐 1GB 以上。

### Q: 如何指定 NDS 共享库路径？

```bash
export NDS_LIBRARY_PATH=/opt/nds/lib/libndskv.so
```

默认为 `libndskv.so`（依赖系统 LD_LIBRARY_PATH）。

### Q: 如何指定 NDS 配置文件？

```bash
export MC_NDS_CONFIG=/opt/nds/conf/nds_config.conf
```

默认为当前执行目录的 `nds_config.conf`。

### Q: 零拷贝读写的要求？

数据必须在 NDS 初始化的内存区域内（contiguous）。如果 slices 不 contiguous（非连续内存），`StoreObjects/LoadObjects` 返回 `INVALID_PARAMS`。使用 `put_from` / `get_into` / `batch_put_from` / `batch_get_into` 时，确保 buffer 地址在 `register_buffer` 注册的范围内。

### Q: 多线程是否安全？

单进程内多线程共享同一个 MooncakeDistributedStore 实例是安全的。但 NDS 是进程级全局单例，不支持多进程共享同一 NDS 实例。