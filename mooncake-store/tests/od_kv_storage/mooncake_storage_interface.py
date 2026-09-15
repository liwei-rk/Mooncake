# SPDX-License-Identifier: Apache-2.0
"""
MooncakeStorageInterface — od_kv_storage 的存储腿适配层
把 XPUStorageInterface 的 xds_* 接口适配到 mooncake DistributedStore（use_od → NDS 盘框）。

WHY 存在：
- od_kv_storage connector 原设计用 kvs_torch 算子直连盘框（GPU 容器缺该 SDK，存储腿从未可用）；
- kv_v8 分支的 MooncakeDistributedStore 已打通 盘框 后端（L2/L3/L4 验证），
  本适配层让 vLLM 的 KV 存取走：vLLM → mooncake store → mooncake_master(use_od) → 盘框。

存储模型（与 xds 原版对齐）：
- key   = "mcs_<block_hash>"（每 token-block 一个 key，K/V 共用）；
- value = VALUE_SIZE（1MB，对应盘框 nsid 的 valueSize）：
    [L0: K(16KB)+V(16KB)][L1: K+V]...[L27: K+V][padding ~128KB]
  storage_offset 参数即 value 内偏移（worker.prepare_xds_input 构造，TP=1 时
  offset = layer*32KB + {0:K, 16KB:V}），直接使用无需换算；
- 写路径：xds_put 按层到达（vLLM 逐层调 save_kv_layer），先 GPU→host staging，
  凑齐全部层后一次 put_from（盘框 value 必须整写，且 key 存在 = 全层就绪，
  天然消除部分写入竞态；盘框不覆盖写，同 hash 重复 put 数据幂等无害）；
- 读路径：xds_get 按层按段请求，value 读缓存命中则直接 GPU 拷贝，否则 get_into
  整 value 到读槽再拷段；
- xds_wait：mooncake 同步语义（原版 cuda 分支同样 no-op），返回记录的失败位图。

vLLM 侧改动仅两处实例化：worker.py / scheduler.py 的 XPUStorageInterface → 本类。
"""
import ctypes
import logging
import mmap
import os
import threading
import time

import numpy as np
import torch

from vllm.logger import init_logger

logger = init_logger(__name__)

try:
    from mooncake.store import MooncakeDistributedStore
except ImportError:
    logger.error("mooncake.store 不可用（需 PYTHONPATH 指向 kv_v8 编译的 mooncake_py）")
    MooncakeDistributedStore = None

# cudaMemcpyKind：pybind 的 torch.cuda.cudart() 直接暴露该枚举值
_CUDA_MEMCPY_HOST_TO_DEVICE = 1
_CUDA_MEMCPY_DEVICE_TO_HOST = 2


class _WriteSlot:
    """一个 block 的聚合写状态：凑齐全部层后整体落盘框"""

    __slots__ = ("staging_offset", "received_layers")

    def __init__(self, staging_offset: int):
        self.staging_offset = staging_offset
        # set of mr_id // 2（相对层号），每层 K+V 两段一次 xds_put 到齐
        self.received_layers = set()


class _ReadSlot:
    """一个 block 的读缓存：整 value 读到 host，逐层按段拷回显存"""

    __slots__ = ("staging_offset", "last_used")

    def __init__(self, staging_offset: int):
        self.staging_offset = staging_offset
        self.last_used = 0.0


class MooncakeStorageInterface:
    """对齐 XPUStorageInterface 的五个接口（xds_init/is_kv_exist/xds_get/xds_put/xds_wait）"""

    def __init__(self, device_type, vllm_config=None):
        self.device_type = device_type
        self._vllm_config = vllm_config

        # master 连接：环境变量（启动脚本注入，与手册 L2 环境一致）
        self._master_rpc = os.environ.get("MC_MASTER_RPC", "")
        self._meta_url = os.environ.get("MC_META_URL", "")
        if not self._master_rpc or not self._meta_url:
            logger.warning(
                "MC_MASTER_RPC / MC_META_URL 未设置，mooncake 存储腿不可用"
            )

        # 布局参数：xds_init 时从调用方得知
        self._base_addrs = []          # 显存池基地址（K0,V0,K1,V1,...，index=mr_id）
        self._total_layers = 0         # 本进程层数（凑齐判据）
        self._is_scheduler_side = False

        self._store = None
        self._staging_mm = None
        self._staging_base = 0
        self._write_pending = {}       # key -> _WriteSlot
        self._read_cache = {}          # key -> _ReadSlot（LRU 淘汰）
        self._batch_results = {}       # batch_io_name -> list[int]（0 成功，>0 失败）
        self._selftest = os.environ.get("MC_STORAGE_SELFTEST", "1") not in ("0", "false")
        self._put_fingerprints = {}    # key -> md5（写入时记录，自检用）
        self._read_fingerprints = {}   # key -> md5（读回时记录）
        # 数据级 dump（排查读回错位用）：put/get 的 value 落盘文件
        self._dump_dir = os.environ.get("MC_STORAGE_DUMP", "")
        self._dump_seq = 0
        self._next_slot = 0
        self._lock = threading.Lock()
        self._cudart = None
        if device_type == "cuda":
            # torch.cuda.cudart() 在部分版本不暴露裸 cudaMemcpy，直接 dlopen libcudart 最稳
            for cand in ("libcudart.so.12", "libcudart.so.11.0", "libcudart.so"):
                try:
                    self._cudart = ctypes.CDLL(cand)
                    self._cudart.cudaMemcpy  # 探测符号
                    break
                except (OSError, AttributeError):
                    self._cudart = None
                    continue
            if self._cudart is None:
                import glob
                for p in glob.glob("/usr/local/lib/python3*/site-packages/"
                                   "nvidia/cuda_runtime/lib/libcudart.so*"):
                    try:
                        self._cudart = ctypes.CDLL(p)
                        self._cudart.cudaMemcpy
                        break
                    except (OSError, AttributeError):
                        self._cudart = None

        self._model_name = ""
        if vllm_config is not None:
            try:
                self._model_name = vllm_config.model_config.served_model_name
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def get_knsid(self):
        # nsid 由 master/客户端环境变量统一控制（OD_KV_NSID），此处仅提供标识
        return 0

    def _str_to_int_id(self, req_id_str: str) -> int:
        import zlib
        return zlib.crc32(req_id_str.encode()) & 0xFFFFFFFF

    @staticmethod
    def _key_of(block_hash) -> str:
        return "mcs_{}".format(int(block_hash))

    def _dump_value(self, key: str, direction: str, slot_off: int, value_size: int):
        """数据级排查：把 put 写入 / get 读回的 value 落盘，跨重启可逐字节对比"""
        if not self._dump_dir:
            return
        try:
            os.makedirs(self._dump_dir, exist_ok=True)
            self._dump_seq += 1
            path = os.path.join(self._dump_dir,
                                "{}_{}_{}".format(direction, key, self._dump_seq))
            with open(path, "wb") as f:
                f.write(self._staging_view[slot_off:slot_off + value_size].tobytes())
        except Exception as e:
            logger.warning("[mc-storage] dump failed: %s", e)

    def _memcpy(self, dst: int, src: int, count: int, kind: int, stream: int = 0):
        """异步 memcpy 到指定 stream：继承 vLLM 的事件依赖（store_stream wait compute event），
        保证 D2H 时该层 kernel 已完成、H2D 后数据对后续 attention 可见"""
        assert self._cudart is not None, "cudaMemcpy 需要 cuda 设备"
        r = self._cudart.cudaMemcpyAsync(ctypes.c_void_p(dst), ctypes.c_void_p(src),
                                         ctypes.c_size_t(count), ctypes.c_int(kind),
                                         ctypes.c_void_p(stream))
        if r != 0:
            raise RuntimeError("cudaMemcpyAsync failed, cudart rc={}".format(r))

    def _stream_sync(self, stream: int):
        r = self._cudart.cudaStreamSynchronize(ctypes.c_void_p(stream))
        if r != 0:
            raise RuntimeError("cudaStreamSynchronize failed, rc={}".format(r))

    def _alloc_slot(self, value_size: int, region: str) -> int:
        """staging 内按 value_size 对齐分配槽位（1MB 倍数，天然 4096 对齐）"""
        off = self._next_slot
        self._next_slot += value_size
        cap = len(self._staging_region(region))
        if self._next_slot > cap:
            raise RuntimeError(
                "staging 区间 {} 不足：需要 {} > {}".format(region, self._next_slot, cap))
        return off

    def _staging_region(self, region: str):
        # 写区在前 60%，读区在后 40%（写 pending 生命周期短、读缓存长）
        total = self._staging_len
        if region == "w":
            return self._staging_view[: int(total * 0.6)]
        return self._staging_view[int(total * 0.6):]

    # ------------------------------------------------------------------
    # xds_init：worker 传 [K0,V0,K1,V1,...]，scheduler 传 [link_tensor]
    # ------------------------------------------------------------------

    def xds_init(self, base_addr, kv_len) -> int:
        try:
            self._base_addrs = list(base_addr)
            n = len(self._base_addrs)
            if n <= 2:
                # scheduler 侧：只需要 store 查存在性，staging 给最小配置
                self._is_scheduler_side = True
                self._total_layers = 1
                staging_len = 8 * 1024 * 1024
            else:
                # worker 侧：K/V 交替 → 层数 = n/2
                self._total_layers = n // 2
                staging_len = 192 * 1024 * 1024

            logger.info(
                "[mc-storage] xds_init: sides=%s layers=%d bases=%d",
                "scheduler" if self._is_scheduler_side else "worker",
                self._total_layers, n)

            # 建 store（连 master）+ host staging + 注册
            self._staging_len = staging_len
            self._staging_mm = mmap.mmap(
                -1, staging_len, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
            self._staging_view = np.frombuffer(
                self._staging_mm, dtype=np.uint8, count=staging_len)
            self._staging_base = self._staging_view.ctypes.data

            self._store = MooncakeDistributedStore()
            rc = self._store.setup(
                local_hostname="127.0.0.1:0",
                metadata_server=self._meta_url,
                global_segment_size=256 * 1024 * 1024,
                local_buffer_size=staging_len,
                protocol="tcp",
                rdma_devices="",
                master_server_addr=self._master_rpc,
            )
            if rc != 0:
                logger.error("[mc-storage] store setup failed rc=%d", rc)
                return 0
            rc = self._store.register_buffer(self._staging_base, staging_len)
            if rc != 0:
                logger.error("[mc-storage] register_buffer failed rc=%d", rc)
                return 0
            logger.info("[mc-storage] store ready (staging %d MB)",
                        staging_len // (1024 * 1024))
            return 1
        except Exception as e:
            logger.exception("[mc-storage] xds_init failed: %s", e)
            return 0

    def _ensure_store(self) -> bool:
        """scheduler 侧惰性初始化：只建 store（连 master）+ 小 staging，用于存在性查询"""
        try:
            staging_len = 16 * 1024 * 1024
            self._staging_len = staging_len
            self._staging_mm = mmap.mmap(
                -1, staging_len, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
            self._staging_view = np.frombuffer(
                self._staging_mm, dtype=np.uint8, count=staging_len)
            self._staging_base = self._staging_view.ctypes.data
            self._store = MooncakeDistributedStore()
            rc = self._store.setup(
                local_hostname="127.0.0.1:0",
                metadata_server=self._meta_url,
                global_segment_size=256 * 1024 * 1024,
                local_buffer_size=staging_len,
                protocol="tcp",
                rdma_devices="",
                master_server_addr=self._master_rpc,
            )
            if rc != 0:
                logger.error("[mc-storage] lazy store setup failed rc=%d", rc)
                self._store = None
                return False
            rc = self._store.register_buffer(self._staging_base, staging_len)
            if rc != 0:
                logger.error("[mc-storage] lazy register_buffer failed rc=%d", rc)
                self._store = None
                return False
            logger.info("[mc-storage] lazy store ready (scheduler side)")
            return True
        except Exception as e:
            logger.exception("[mc-storage] lazy store init failed: %s", e)
            self._store = None
            return False

    # ------------------------------------------------------------------
    # is_kv_exist（scheduler 侧）：前缀连续命中数
    # ------------------------------------------------------------------

    def is_kv_exist(self, req_id: str, block_hash_list) -> tuple:
        try:
            if not block_hash_list:
                return 0, 0
            # scheduler 侧（cuda）不会走 xds_init（那在 vllm 的 npu 分支里），这里惰性建 store
            if self._store is None and not self._ensure_store():
                return -1, 0
            keys = [self._key_of(h) for h in block_hash_list]
            results = self._store.batch_is_exist(keys)
            hit = 0
            for r in results:
                if r == 1:
                    hit += 1
                else:
                    break  # 前缀命中语义：遇到第一个不存在即止
            logger.info("[mc-storage] is_kv_exist: %d/%d hit",
                        hit, len(block_hash_list))
            return 0, hit
        except Exception as e:
            logger.exception("[mc-storage] is_kv_exist failed: %s", e)
            return -1, 0

    # ------------------------------------------------------------------
    # xds_put：按层到达 → staging 聚合 → 凑齐全层一次写
    # ------------------------------------------------------------------

    def xds_put(self, req_id: str, mr_id_list, block_hash_list, hbm_addr_list,
                storage_offset_list, length_list, stream_id: int) -> int:
        results = [0] * len(mr_id_list)
        try:
            value_size = self._value_size()
            # WHY 全设备同步：od_kv_storage 的 end_event fallback record 不可靠
            #（save_kv_layer 注释自述 "reshape_and_cache is async fun"），
            # compute kernel 与 store_stream 间无保证的依赖链；D2H 前强制等
            # GPU 全部就绪，确保读到该层最终 KV。跑通优先，后续可换精确 event 链。
            if self._cudart is not None:
                self._cudart.cudaDeviceSynchronize()
            with self._lock:
                for i in range(len(mr_id_list)):
                    mr_id = mr_id_list[i]
                    key = self._key_of(block_hash_list[i])
                    # GPU → host staging 对应槽位（storage_offset 即 value 内偏移）
                    slot = self._write_pending.get(key)
                    if slot is None:
                        off = self._alloc_slot(value_size, "w")
                        slot = _WriteSlot(off)
                        # 尾部 padding 清零（1MB value 必须整写）
                        self._staging_view[off:off + value_size] = 0
                        self._write_pending[key] = slot
                    try:
                        self._memcpy(
                            self._staging_base + slot.staging_offset + storage_offset_list[i],
                            self._base_addrs[mr_id] + hbm_addr_list[i],
                            length_list[i],
                            _CUDA_MEMCPY_DEVICE_TO_HOST, stream_id)
                        slot.received_layers.add(mr_id // 2)
                    except Exception as e:
                        logger.error("[mc-storage] put D2H failed seg %d: %s", i, e)
                        results[i] = 1
                        continue

                    # 凑齐全部层 → 等 stream 上 D2H 完成 → 整 value 落盘框
                    if len(slot.received_layers) >= self._total_layers:
                        try:
                            self._stream_sync(stream_id)
                            self._dump_value(key, "put", slot.staging_offset, value_size)
                            rc = self._store.put_from(
                                key, self._staging_base + slot.staging_offset, value_size)
                            if rc != 0:
                                logger.error("[mc-storage] put_from %s rc=%d", key, rc)
                                results[i] = 1
                            elif self._selftest:
                                # 自检：记录写入 value 的指纹，get 时对比读回是否一致
                                import hashlib as _h
                                self._put_fingerprints[key] = _h.md5(
                                    self._staging_view[slot.staging_offset:
                                                       slot.staging_offset + value_size]
                                ).hexdigest()
                                if len(self._put_fingerprints) > 64:
                                    self._put_fingerprints.pop(next(iter(self._put_fingerprints)))
                        finally:
                            del self._write_pending[key]
            self._batch_results[req_id] = results
            return 0
        except Exception as e:
            logger.exception("[mc-storage] xds_put failed: %s", e)
            return -1

    # ------------------------------------------------------------------
    # xds_get：读缓存整 value → 按段拷回显存
    # ------------------------------------------------------------------

    def xds_get(self, req_id: str, mr_id_list, block_hash_list, hbm_addr_list,
                storage_offset_list, length_list, stream_id: int) -> int:
        results = [0] * len(mr_id_list)
        try:
            value_size = self._value_size()
            with self._lock:
                for i in range(len(mr_id_list)):
                    mr_id = mr_id_list[i]
                    key = self._key_of(block_hash_list[i])
                    slot = self._read_cache.get(key)
                    if slot is None:
                        off = self._alloc_slot(value_size, "r")
                        slot = _ReadSlot(off)
                        rc = self._store.get_into(
                            key, self._staging_base + off, value_size)
                        if rc <= 0:
                            logger.error("[mc-storage] get_into %s rc=%d", key, rc)
                            results[i] = 1
                            continue
                        self._dump_value(key, "get", off, value_size)
                        if self._selftest and key in self._put_fingerprints:
                            # 自检：读回 value 与写入指纹对比（重启后 _put_fingerprints 为空，
                            # 此时记录首读指纹供后续层比对）
                            import hashlib as _h
                            fp = _h.md5(self._staging_view[off:off + value_size]).hexdigest()
                            if self._read_fingerprints.get(key) is None:
                                self._read_fingerprints[key] = fp
                                if fp != self._put_fingerprints[key]:
                                    logger.warning(
                                        "[mc-storage] SELFTEST key=%s readback != written! "
                                        "read=%s put=%s", key, fp[:8],
                                        self._put_fingerprints[key][:8])
                                else:
                                    logger.info("[mc-storage] SELFTEST key=%s readback OK", key)
                        # 淘汰最老的读槽
                        if len(self._read_cache) >= 64:
                            oldest = min(self._read_cache.values(),
                                        key=lambda s: s.last_used)
                            for k, s in list(self._read_cache.items()):
                                if s is oldest:
                                    del self._read_cache[k]
                                    break
                        self._read_cache[key] = slot
                    slot.last_used = time.time()
                    try:
                        self._memcpy(
                            self._base_addrs[mr_id] + hbm_addr_list[i],
                            self._staging_base + slot.staging_offset + storage_offset_list[i],
                            length_list[i],
                            _CUDA_MEMCPY_HOST_TO_DEVICE, stream_id)
                    except Exception as e:
                        logger.error("[mc-storage] get H2D failed seg %d: %s", i, e)
                        results[i] = 1
            # 等 H2D 全部落入显存（xds_wait 同步语义，返回即数据可见）
            try:
                self._stream_sync(stream_id)
            except Exception as e:
                logger.error("[mc-storage] get stream sync failed: %s", e)
            self._batch_results[req_id] = results
            return 0
        except Exception as e:
            logger.exception("[mc-storage] xds_get failed: %s", e)
            return -1

    # ------------------------------------------------------------------
    # xds_wait：同步语义，返回每段成败（与原版 cuda 分支一致）
    # ------------------------------------------------------------------

    def xds_wait(self, req_id: str, output_len: int, stream_id: int):
        results = self._batch_results.pop(req_id, None)
        if results is None:
            results = [0] * output_len
        return 0, results[:output_len]

    # ------------------------------------------------------------------

    def _value_size(self) -> int:
        # 1MB：与新 nsid(147815026) 的 valueSize 一致；后续 TP>1 时按层布局放大
        return 1024 * 1024
