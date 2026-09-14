# Mooncake NDS 盘框多进程链路复现手册

> 目标环境：GPU 服务器 128（51.36.133.128，Ubuntu 22.04，主机直接编译运行）
> 盘框：192.168.133.197:33050（RoCE v2，mlx5_2/mlx5_3 双网卡）
> 分支：kv_v8（含 DISK 副本 file_path 修复 ef7fdc8，**必须包含此提交**，否则跨客户端盘框读全部失败）

---

## 0. 跑通指标（先看这个）

三层验证全部通过即算"多进程链路跑通"：

| 层 | 命令 | 跑通标志 |
|---|---|---|
| L1 C++ 全链路 | `nds_client_test --protocol=tcp`（配 `OD_KV_NSID`） | `[  PASSED  ] 9 tests.` |
| L2 单进程数据正确性 | `nds_data_correctness_test.py --block-size=33554432 --batch-size=2 --global-segment-size 256` | 五个 Phase 全过（E2E_EXIT=0）、`Disk-only BatchGet data verified OK`；日志中 `StoreObjects failed` 计数为 **0**。`--global-segment-size 256` 必带：默认 64MB 塞不下 2×32MB 批量，会假报 insufficient space |
| L3 多进程压测 | `nds_stress_test.py --num-workers 2 --duration 15 --block-size 33554432 --batch-size 2` | `Total errors: 0`、`Error rate: 0.000%`、`Avg bandwidth ≥ 5 GB/s`（实测均值 7.2，峰值 13.26）|
| L4 官方 E2E | `e2e_rand_test --run_sec=30 --etcd_endpoints=127.0.0.1:2379`（见第 9 节） | `[  PASSED  ] 1 test.`、日志 `TEST_ERROR` 计数为 **0** |

L3 两个 worker 是**独立进程**（各自持有独立 NDS 实例），writer 写的 key 由 reader 进程读 —— 这就是"多进程链路"的直接证据。

## 1. 前置条件

- 服务器无外网（三个代理均不可达）：**所有下载在本地完成，经 SFTP 上传**（tar 包 + deb 文件）
- 磁盘余量 ≥ 10G（编译约耗 7G）
- RDMA 就绪：`ibv_devinfo` 可见 mlx5_2/mlx5_3；PFC `pfc 1,0,0,1,0,0,0,0` 已配置（NDS 用 SL=0，priority 0 的 PFC 不能关）
- 可用 nsid：3851719013（主用）/ 3069224783（备用）

## 2. 上传源码

本地（有外网）：

```bash
git clone -b kv_v8 https://github.com/liwei-rk/Mooncake.git
git submodule update --init --depth 1          # extern/pybind11
tar --exclude="Mooncake-kv_v8/.git" --exclude="Mooncake-kv_v8/extern/pybind11/.git" \
    -czf mooncake_kv_v8.tar.gz Mooncake-kv_v8
```

SFTP 上传 tar 到服务器 `/home/yyc/` 后解压：

```bash
tar xzf mooncake_kv_v8.tar.gz -C /home/yyc/    # 得到 /home/yyc/Mooncake-kv_v8
```

## 3. 安装编译依赖

### 3.1 dpkg 锁

`apt-get install` 若报 `dpkg lock ... unattended-upgr`：直接 `kill -9` 该进程（不要用 `systemctl stop`，会卡住等升级自然结束）：

```bash
pkill -9 -f unattended-upgrade; dpkg --configure -a
```

### 3.2 系统包（大部分预装，缺的走 3.3 离线通道）

```bash
apt-get install -y build-essential cmake git wget unzip libibverbs-dev libgoogle-glog-dev \
  libgtest-dev libjsoncpp-dev libunwind-dev libnuma-dev libpython3-dev libboost-all-dev \
  libssl-dev libgrpc-dev libgrpc++-dev libprotobuf-dev libyaml-cpp-dev protobuf-compiler-grpc \
  libcurl4-openssl-dev libhiredis-dev liburing-dev libjemalloc-dev libmsgpack-dev \
  libzstd-dev libasio-dev libxxhash-dev pkg-config patchelf
```

### 3.3 离线 deb 通道（apt 联网失败时的通用解法）

利用服务器上 apt 的源缓存生成精确 URL，本地下载后上传安装：

```bash
# 服务器：列出缺失依赖的 .deb URL
apt-get install --print-uris -y -qq <缺失包名> | grep -oP "^'\K[^']+"
# 本地：curl -sSL -o <文件名> <URL>，SFTP 传到 /tmp/debs/
# 服务器：DEBIAN_FRONTEND=noninteractive apt-get install -y /tmp/debs/*.deb
```

实测缺过的包：`libprotobuf-dev`（含 lite23/protoc 闭包 5 个）、`libyaml-cpp-dev`、`libxxhash-dev`、`patchelf`、`libzstd-dev`、`libmsgpack-dev`。libboost 全家桶**不需要**（mooncake 本体不用 boost）。

### 3.4 yalantinglibs 0.5.7（header-only，秒装）

本地下载 `https://github.com/alibaba/yalantinglibs/archive/refs/tags/0.5.7.zip`（Windows curl 加 `--ssl-no-revoke`），上传后：

```bash
cd /home/yyc && unzip -q yalantinglibs-0.5.7.zip && cd yalantinglibs-0.5.7
mkdir -p build && cd build
cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF -DCMAKE_BUILD_TYPE=Release
make -j 64 && cmake --install .     # 头文件装到 /usr/local/include/ylt/
```

### 3.5 pybind11 子模块

本地 `git submodule update --init` 后把 `extern/pybind11` 打 tar 上传解压到 `Mooncake-kv_v8/extern/pybind11`（初版 tar 时子模块为空目录会直接 cmake 失败）。

## 4. 编译

```bash
cd /home/yyc/Mooncake-kv_v8 && mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release \
  -DWITH_TE=ON -DWITH_STORE=ON \
  -DWITH_P2P_STORE=OFF -DWITH_EP=OFF -DWITH_RUST_EXAMPLE=OFF \
  -DUSE_ETCD=OFF -DUSE_CUDA=OFF -DUSE_ASCEND=OFF \
  -DUSE_CXL=OFF -DUSE_EFA=OFF -DUSE_MNNVL=OFF \
  -DUSE_HTTP=ON -DSTORE_USE_JEMALLOC=OFF \
  -DBUILD_UNIT_TESTS=ON -DBUILD_EXAMPLES=OFF
make -j 64        # 首次约 4 分钟，增量约 1 分钟；80 个 target
```

产物（都在 `build/` 下）：

```
mooncake-store/src/mooncake_master                    # Master 二进制
mooncake-integration/engine.cpython-312-*.so          # TransferEngine 绑定
mooncake-integration/store.cpython-312-*.so           # Store 绑定（含 NDS 后端）
mooncake-store/tests/nds_client_test                  # C++ 测试
```

NDS 后端编入的自检：`strings mooncake-store/src/mooncake_master | grep libndskv` 应能看到 `Successfully loaded libndskv.so`。

## 5. 运行时物料

### 5.1 NDS 运行时目录（全部复制，不动 pynds_kv 原件）

```bash
mkdir -p /home/yyc/mooncake_nds_runtime/NDS_bin /home/yyc/mooncake_nds_runtime/NDS_lib
cp /home/yyc/pynds_kv/vllm/NDS_bin/libndskv.so   /home/yyc/mooncake_nds_runtime/NDS_bin/
cp /home/yyc/pynds_kv/vllm/NDS_lib/libndsclient.so /home/yyc/mooncake_nds_runtime/NDS_lib/
cp /home/yyc/pynds_kv/nds_config.conf    /home/yyc/mooncake_nds_runtime/
cp /home/yyc/pynds_kv/nds_wrapper.conf   /home/yyc/mooncake_nds_runtime/
```

自检三点：

```bash
# ① Mooncake 依赖的 6 个 c_* 符号都在
nm -D --defined-only /home/yyc/mooncake_nds_runtime/NDS_bin/libndskv.so | grep ' c_'
# ② RUNPATH=$ORIGIN/../NDS_lib 能解析 libndsclient.so
ldd /home/yyc/mooncake_nds_runtime/NDS_bin/libndskv.so | grep ndsclient
# ③ c_init 是 3 参数签名（memAddr, length, config_path）
grep 'c_init' /home/yyc/pynds_kv/vllm/NDS3.0/nds_c_adapter.cpp
```

### 5.2 Python 包组装（store.so 实体拷贝 + 绝对 rpath）

```bash
mkdir -p /home/yyc/mooncake_py/mooncake
cp /home/yyc/Mooncake-kv_v8/mooncake-wheel/mooncake/__init__.py /home/yyc/mooncake_py/mooncake/
cp /home/yyc/Mooncake-kv_v8/build/mooncake-integration/store.cpython-312-*.so /home/yyc/mooncake_py/mooncake/store.so
patchelf --set-rpath '/home/yyc/Mooncake-kv_v8/build/mooncake-asio' /home/yyc/mooncake_py/mooncake/store.so
# engine.so 同理补 rpath（$ORIGIN/../mooncake-asio:/root/miniconda3/lib）
PYTHONPATH=/home/yyc/mooncake_py python3 -c "from mooncake.store import MooncakeDistributedStore; print('OK')"
```

> 注意：**不能用软链**（`$ORIGIN` 不跟随 symlink，rpath 会解析落空）。

## 6. 三层验证

统一环境变量：

```bash
export PYTHONPATH=/home/yyc/mooncake_py
export NDS_LIBRARY_PATH=/home/yyc/mooncake_nds_runtime/NDS_bin/libndskv.so
export MC_NDS_CONFIG=/home/yyc/mooncake_nds_runtime/nds_config.conf
export OD_KV_NSID=3851719013
export no_proxy=127.0.0.1,localhost
```

### L1 C++ 全链路（InProcMaster，无需外部服务）

```bash
cd /home/yyc/Mooncake-kv_v8/build
./mooncake-store/tests/nds_client_test --protocol=tcp
```

**跑通标志**：`[ PASSED ] 9 tests.`，日志含 `NDS nsid: 3851719013`。
（注意：此测试的读全命中 MEMORY 副本，**不能**作为盘框读写正确性的证据 —— 那 9 个用例全部通过也掩盖过 file_path bug。）

### L2 单进程数据正确性（自动拉起 master）

```bash
cd /home/yyc/Mooncake-kv_v8/mooncake-store/tests
python3 nds_data_correctness_test.py --protocol=tcp --block-size=33554432 --batch-size=2
```

**跑通标志**：

```
Disk-only BatchGet: all 2 keys OK
Disk-only BatchGet data verified OK
```

且日志 `grep -c 'StoreObjects failed'` 为 **0**。
`--block-size=33554432` 必须带：盘框 value_size 固定 32MB，小于它的写入需要 wrapper 补齐（要求所在 MR ≥ 32MB），默认 4096 的 block 会触发 `prepareNdsReqsBatch failed`。32MB 恰好等于 value_size，完全绕开 padding。

### L3 多进程压测

```bash
cp /home/yyc/pynds_kv/key.txt .
python3 nds_stress_test.py --operation-mode mixed --num-workers 2 --duration 15 \
  --block-size 33554432 --batch-size 2 --nsid 3851719013 --protocol tcp
```

**跑通标志**：

```
Total ops:                ~2000
Total errors:                0
Error rate:              0.000%
Avg bandwidth:          ≥ 5 GB/s（实测 7.20，峰值 13.26）
```

worker 自动使用唯一 local_hostname（`:0` → 随机空闲端口），**不要**改成固定相同端口 —— 两个客户端 segment 同名会导致远端读地址错乱（TCP 服务端 EFAULT）。

## 7. 已知问题与规避

| 问题 | 现象 | 规避/状态 |
|---|---|---|
| DISK 副本 file_path 为空 | 跨客户端盘框读 1842 | **已修复**（ef7fdc8），必须用含此提交的分支 |
| block-size < 32MB | `prepareNdsReqsBatch failed (empty reqs)` | 用 `--block-size=33554432`，或保证写入 buffer 所在 MR ≥ 32MB |
| segment 容量不足 | `BatchPut failed ... insufficient space`、个别 key 数据不落盘 | 调大 `--global-segment-size`（默认 64MB，32MB 对象×2 即触顶）|
| 偶发第二个 NDS 实例 c_init 挂死 | 裸进程也会复现，与 Mooncake 无关 | 重跑即可；根因在 NDS 客户端库多实例竞态，待查 |
| Mooncake reader 跨进程 NDS 批量读偶发挂死 | `nds_kv_wait` 内 `cpu_relax` 自旋（NDS_client.c:1843） | 调查中：裸进程同场景已证可用，嫌疑在 Mooncake 进程内多线程环境与 NDS poll 线程（start_core=2）的相互作用 |
| 盘框不覆盖写 | 对已存在 key 的写入静默 no-op | 测试 key 用随机/唯一后缀；blockId 来自 std::hash(key)，与 pynds key.txt 的数字 key 天然不冲突 |

## 8. 清理与陷阱

```bash
pkill -9 -x mooncake_master    # 杀 master（-x 按进程名精确匹配）
pkill -9 -f unattended-upgrade # 处理 dpkg 锁
```

**pkill -f 自杀陷阱**：`pkill -f <模式>` 会匹配 SSH/脚本自身命令行（脚本文本里含同样字符串）。把清理动作写进**服务器端脚本文件**（如 `/tmp/kill_nds.sh`）再 `bash /tmp/kill_nds.sh` 执行。模式加 `[e]` 括号技巧仅在同一命令行没有其他真实匹配时有效。

僵尸 master（`<defunct>`）无害，不占端口，忽略即可。

## 9. 官方 E2E（e2e_rand_test，HA + etcd）

`mooncake-store/tests/e2e/` 是 Mooncake 官方系统级端到端测试：测试框架自动拉起 **HA master 进程**（`--enable-ha=true`，leader 选举走 etcd），2 个独立 client 进程做随机 put/get/delete 并校验数据。

### 9.1 与第 3-4 节构建的差异

| 项 | 说明 |
|---|---|
| Go 1.23.8 | HA 的 etcd helper 用 **Go c-shared wrapper**（libetcd_wrapper.so）。服务器断网时：本地下载 go1.23.8（阿里云 golang 镜像）→ 在 `mooncake-common/etcd` 目录 `GOPROXY=http://mirrors.aliyun.com/goproxy/,https://goproxy.cn,direct go mod tidy && go mod vendor` → 整个 etcd 目录（含 vendor/）上传替换 → patch `etcd/CMakeLists.txt` 把 `bash -c "go mod tidy" && bash -c "go build` 替换成 `bash -c "go build -mod=vendor`（跳过联网 tidy）→ 服务器装 Go（`tar -C /usr/local -xzf go1.23.8.linux-amd64.tar.gz`），构建时 `export PATH=/usr/local/go/bin:$PATH GOFLAGS=-mod=vendor GOPROXY=off` |
| cmake 参数 | 在第 4 节基础上加 `-DSTORE_USE_ETCD=ON`（USE_ETCD/USE_ETCD_LEGACY 不需要）|
| etcd 服务端 | 华为云镜像 `https://mirrors.huaweicloud.com/etcd/v3.5.21/etcd-v3.5.21-linux-amd64.tar.gz`，单机起：`etcd --name nds-e2e --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379 --listen-peer-urls http://127.0.0.1:2380 --initial-advertise-peer-urls http://127.0.0.1:2380 --initial-cluster nds-e2e=http://127.0.0.1:2380` |
| TE metadata | E2E client 的 TE 需要 `--engine_meta_url=http://127.0.0.1:8080/metadata`：另起一个 master `--enable_http_metadata_server=true --http_metadata_server_port=8080 --rpc_port=50098 --metrics_port=18082`（**metrics 端口必须换**，默认 9003 会和 E2E master 冲突）|

### 9.2 运行

```bash
cd /home/yyc/Mooncake-kv_v8/build
./mooncake-store/tests/e2e/e2e_rand_test --run_sec=30 \
  --etcd_endpoints=127.0.0.1:2379 --protocol=tcp \
  --engine_meta_url=http://127.0.0.1:8080/metadata --rand_seed=42
```

**跑通标志**：`[  PASSED  ] 1 test.`（E2E_EXIT=0），日志 `TEST_ERROR` 计数 0，master 计量出现 `PutEnd=34/34` 级别的成功计数。

### 9.3 已修的坑（提交 3a65c13）

E2E harness 用 `--enable-ha=true` 启动 master 但不传 `--cluster_id`：master 默认 `mooncake_cluster`，把 view 写到 `mooncake-store/mooncake_cluster/master_view`；client 空 namespace 解析成 `mooncake`，读 `mooncake-store/mooncake/master_view` → 永远找不到 master view。已给 process_handler.cpp 显式加 `--cluster_id=mooncake`。
