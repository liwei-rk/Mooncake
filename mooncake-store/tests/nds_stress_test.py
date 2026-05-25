import argparse
import gc
import mmap
import numpy as np
import time
import threading
import math
import sys
import os
import logging
from collections import defaultdict
from mooncake.store import MooncakeDistributedStore

GB = 1024**3
MB = 1024**2

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

stop_event = threading.Event()

OPERATION_MODE = "batch_put"
BLOCK_SIZE = 128 * 1024
BATCH_SIZE = 128
NUM_THREADS = 8
TEST_DURATION = 30
MONITOR_INTERVAL = 1

GLOBAL_SEGMENT_SIZE_MB = 3200
LOCAL_BUFFER_SIZE_MB = 512

pre_loaded_keys = []

class ThreadStats:
    def __init__(self):
        self.io_count = 0
        self.total_bytes = 0
        self.total_latency = 0.0
        self.success_ops = 0
        self.error_count = 0

        self.all_io_count = 0
        self.all_total_bytes = 0
        self.all_total_latency = 0.0
        self.all_success_ops = 0
        self.all_error_count = 0

    def update(self, success, bytes_transferred, latency):
        self.io_count += 1
        self.all_io_count += 1
        self.total_bytes += bytes_transferred
        self.all_total_bytes += bytes_transferred
        self.total_latency += latency
        self.all_total_latency += latency

        if success:
            self.success_ops += 1
            self.all_success_ops += 1
        else:
            self.error_count += 1
            self.all_error_count += 1

    def reset_periodic(self):
        self.io_count = 0
        self.total_bytes = 0
        self.total_latency = 0.0
        self.success_ops = 0
        self.error_count = 0


class GlobalStats:
    def __init__(self, num_threads):
        self.start_time = time.time()
        self.bw_start_time = time.time()
        self.thread_stats = [ThreadStats() for _ in range(num_threads)]

    def add_batch_stats(self, thread_idx, success, total_size, latency):
        self.thread_stats[thread_idx].update(success, total_size, latency)

    def get_all_ops(self):
        return sum(s.all_io_count for s in self.thread_stats)

    def get_all_bytes(self):
        return sum(s.all_total_bytes for s in self.thread_stats)

    def get_all_errors(self):
        return sum(s.all_error_count for s in self.thread_stats)

    def get_all_avg_latency(self):
        total_ops = sum(s.all_io_count for s in self.thread_stats)
        total_lat = sum(s.all_total_latency for s in self.thread_stats)
        if total_ops == 0:
            return 0
        return total_lat / total_ops

    def snapshot_and_reset(self):
        elapsed = time.time() - self.bw_start_time
        total_bytes = sum(s.total_bytes for s in self.thread_stats)
        total_ops = sum(s.io_count for s in self.thread_stats)
        total_lat = sum(s.total_latency for s in self.thread_stats)
        total_errors = sum(s.error_count for s in self.thread_stats)
        avg_lat = total_lat / total_ops if total_ops > 0 else 0
        bw = total_bytes / elapsed / GB if elapsed > 0 else 0
        self.bw_start_time = time.time()
        for s in self.thread_stats:
            s.reset_periodic()
        return bw, avg_lat, total_ops, total_errors


def load_keys_from_file(filepath="key.txt"):
    global pre_loaded_keys
    print("    Loading block keys from {}...".format(filepath))
    try:
        with open(filepath, "r") as f:
            lines = f.readlines()
            if not lines:
                raise ValueError("key.txt is empty")
            pre_loaded_keys = []
            for line in lines:
                line = line.strip()
                if line:
                    pre_loaded_keys.append(line)
            if not pre_loaded_keys:
                raise ValueError("No valid keys found")
            print("    Successfully loaded {} keys".format(len(pre_loaded_keys)))
    except FileNotFoundError:
        print("    Warning: key.txt not found")
    except Exception as e:
        print("    Failed to load keys: {}".format(e))


def get_next_batch_keys(current_index, count):
    total_keys = len(pre_loaded_keys)
    if total_keys == 0:
        return [], current_index
    keys = []
    for i in range(count):
        idx = (current_index + i) % total_keys
        keys.append(pre_loaded_keys[idx])
    new_index = (current_index + count) % total_keys
    return keys, new_index


def batch_put_worker(thread_idx, store, buffer_ptr, block_size, batch_size, global_stats):
    current_key_index = 0
    total_batch_bytes = batch_size * block_size

    while not stop_event.is_set():
        try:
            keys, current_key_index = get_next_batch_keys(current_key_index, batch_size)

            buffer_ptrs = []
            sizes = []
            for i in range(len(keys)):
                offset = i * block_size
                buffer_ptrs.append(buffer_ptr + offset)
                sizes.append(block_size)

            start_time = time.time()
            ret_codes = store.batch_put_from(keys, buffer_ptrs, sizes)
            end_time = time.time()
            latency = end_time - start_time

            all_success = all(rc == 0 for rc in ret_codes)
            global_stats.add_batch_stats(thread_idx, all_success, total_batch_bytes, latency)

            if not all_success:
                failed_count = sum(1 for rc in ret_codes if rc != 0)
                if failed_count > 3:
                    logger.warning("Thread {} batch_put: {} keys failed".format(
                        thread_idx, failed_count))
        except Exception as e:
            logger.error("Thread {} batch_put exception: {}".format(thread_idx, e))
            global_stats.add_batch_stats(thread_idx, False, 0, 0)
            stop_event.set()


def batch_get_worker(thread_idx, store, buffer_ptr, block_size, batch_size, global_stats):
    current_key_index = 0
    total_batch_bytes = batch_size * block_size

    while not stop_event.is_set():
        try:
            keys, current_key_index = get_next_batch_keys(current_key_index, batch_size)

            buffer_ptrs = []
            sizes = []
            for i in range(len(keys)):
                offset = i * block_size
                buffer_ptrs.append(buffer_ptr + offset)
                sizes.append(block_size)

            start_time = time.time()
            ret_codes = store.batch_get_into(keys, buffer_ptrs, sizes)
            end_time = time.time()
            latency = end_time - start_time

            all_success = all(rc > 0 for rc in ret_codes)
            global_stats.add_batch_stats(thread_idx, all_success, total_batch_bytes, latency)

            if not all_success:
                failed_count = sum(1 for rc in ret_codes if rc <= 0)
                if failed_count > 3:
                    logger.warning("Thread {} batch_get: {} keys failed".format(
                        thread_idx, failed_count))
        except Exception as e:
            logger.error("Thread {} batch_get exception: {}".format(thread_idx, e))
            global_stats.add_batch_stats(thread_idx, False, 0, 0)
            stop_event.set()


def monitor_thread(global_stats):
    print("    Monitor thread started, reporting every {}s...".format(MONITOR_INTERVAL))
    while not stop_event.is_set():
        try:
            time.sleep(MONITOR_INTERVAL)
            if stop_event.is_set():
                break
            elapsed = time.time() - global_stats.start_time
            bw, avg_lat, total_ops, total_errors = global_stats.snapshot_and_reset()

            error_rate = 0
            if total_ops > 0:
                error_rate = total_errors / total_ops * 100

            print("[Monitor] {:6.1f}s - BW: {:5.2f} GB/s, AvgLat: {:.6f}s, "
                  "Errors: {:3d}, ErrorRate: {:.1f}%".format(
                      elapsed, bw, avg_lat, total_errors, error_rate))
        except KeyboardInterrupt:
            print("    Monitor thread interrupted")
            stop_event.set()
            break
        except Exception as e:
            print("    Monitor thread exception: {}".format(e))
            stop_event.set()
            break


def print_final_report(global_stats, args):
    print("\n" + "=" * 80)
    print("NDS STRESS TEST - FINAL REPORT".center(80))
    print("=" * 80)
    elapsed = time.time() - global_stats.start_time
    print("Test duration:       {:.2f}s".format(elapsed))
    print("Operation mode:      {}".format(OPERATION_MODE))
    print("Batch size:          {}".format(args.batch_size))
    print("Block size:          {} ({:.2f} MB)".format(args.block_size, args.block_size / MB))
    print("Num threads:         {}".format(args.num_threads))
    print("Keys from key.txt:   {}".format(len(pre_loaded_keys)))
    print("-" * 80)

    total_bytes = global_stats.get_all_bytes()
    total_errors = global_stats.get_all_errors()
    total_ops = global_stats.get_all_ops()
    avg_lat = global_stats.get_all_avg_latency()

    bw_gbs = total_bytes / elapsed / GB if elapsed > 0 else 0
    error_rate = total_errors / max(total_ops, 1) * 100

    print("Total ops:           {:12d}".format(total_ops))
    print("Total bytes:         {:12d} ({:.2f} GB)".format(total_bytes, total_bytes / GB))
    print("Total errors:        {:12d}".format(total_errors))
    print("Error rate:          {:12.3f}%".format(error_rate))
    print("-" * 80)
    print("Avg bandwidth:       {:5.2f} GB/s".format(bw_gbs))
    print("Avg latency:         {:.6f}s".format(avg_lat))
    print("=" * 80)


def parse_args():
    parser = argparse.ArgumentParser(description="NDS Stress Test for Mooncake Client")
    parser.add_argument("--operation-mode", type=str, default="batch_put",
                        choices=["batch_put", "batch_get", "mixed"],
                        help="Operation mode: batch_put, batch_get, or mixed")
    parser.add_argument("--block-size", type=int, default=128 * 1024,
                        help="Size of a single block in bytes")
    parser.add_argument("--batch-size", type=int, default=128,
                        help="Number of keys per batch operation")
    parser.add_argument("--num-threads", type=int, default=8,
                        help="Number of worker threads")
    parser.add_argument("--duration", type=int, default=30,
                        help="Test duration in seconds")
    parser.add_argument("--monitor-interval", type=int, default=1,
                        help="Monitor report interval in seconds")

    parser.add_argument("--protocol", type=str, default="tcp",
                        help="Transfer protocol")
    parser.add_argument("--device-name", type=str, default="",
                        help="RDMA device name (empty for TCP)")
    parser.add_argument("--local-hostname", type=str, default="127.0.0.1:12345",
                        help="Local hostname with port")
    parser.add_argument("--metadata-server", type=str,
                        default="http://127.0.0.1:8080/metadata",
                        help="Metadata server address")
    parser.add_argument("--master-server", type=str, default="127.0.0.1:50051",
                        help="Master server address")
    parser.add_argument("--global-segment-size", type=int, default=3200,
                        help="Global segment size in MB")
    parser.add_argument("--local-buffer-size", type=int, default=512,
                        help="Local buffer size in MB")

    parser.add_argument("--key-file", type=str, default="key.txt",
                        help="Path to key.txt file containing block keys")
    return parser.parse_args()


def run_stress_test(args):
    global OPERATION_MODE, BLOCK_SIZE, BATCH_SIZE, NUM_THREADS, TEST_DURATION, MONITOR_INTERVAL

    OPERATION_MODE = args.operation_mode
    BLOCK_SIZE = args.block_size
    BATCH_SIZE = args.batch_size
    NUM_THREADS = args.num_threads
    TEST_DURATION = args.duration
    MONITOR_INTERVAL = args.monitor_interval

    print("=" * 80)
    print("NDS STRESS TEST".center(80))
    print("=" * 80)
    print("Operation mode:      {}".format(OPERATION_MODE))
    print("Batch size:          {}".format(BATCH_SIZE))
    print("Block size:          {} ({:.2f} MB)".format(BLOCK_SIZE, BLOCK_SIZE / MB))
    print("Num threads:         {}".format(NUM_THREADS))
    print("Test duration:       {}s".format(TEST_DURATION))
    print("Protocol:            {}".format(args.protocol))
    print("Metadata server:     {}".format(args.metadata_server))
    print("=" * 80)

    load_keys_from_file(args.key_file)
    if not pre_loaded_keys:
        print("ERROR: No keys loaded. Cannot run test.")
        return

    total_threads = NUM_THREADS
    buffer_size_per_thread = BATCH_SIZE * BLOCK_SIZE
    total_buffer_size = buffer_size_per_thread * total_threads

    print(">>> Phase I: Allocate page-aligned buffer ({:.2f} MB total)".format(
        total_buffer_size / MB))
    mm = mmap.mmap(-1, total_buffer_size,
                   flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
    buf = np.frombuffer(mm, dtype=np.uint8, count=total_buffer_size)
    base_addr = buf.__array_interface__["data"][0]
    print("    Buffer base address = 0x{:x}".format(base_addr))
    print("    4096-aligned? {}".format("YES" if base_addr % 4096 == 0 else "NO"))

    pattern = (np.arange(BLOCK_SIZE, dtype=np.uint32) % 251).astype(np.uint8)

    print(">>> Phase II: Setup MooncakeDistributedStore clients")
    stores = []
    registered_ptrs = []
    for i in range(total_threads):
        store = MooncakeDistributedStore()

        global_segment_size = args.global_segment_size * MB
        local_buffer_size = args.local_buffer_size * MB

        retcode = store.setup(
            args.local_hostname,
            args.metadata_server,
            global_segment_size,
            local_buffer_size,
            args.protocol,
            args.device_name,
            args.master_server
        )

        if retcode:
            print("ERROR: Store setup failed for thread {}, retcode={}".format(i, retcode))
            mm.close()
            return

        thread_buf_start = i * buffer_size_per_thread
        thread_buf_end = thread_buf_start + buffer_size_per_thread
        thread_buf = buf[thread_buf_start:thread_buf_end]
        thread_buf[:] = pattern
        thread_buf_ptr = thread_buf.ctypes.data

        retcode = store.register_buffer(thread_buf_ptr, buffer_size_per_thread)
        if retcode:
            print("ERROR: Buffer registration failed for thread {}, retcode={}".format(
                i, retcode))
            mm.close()
            return

        stores.append(store)
        registered_ptrs.append(thread_buf_ptr)
        print("    Thread {} client setup + buffer registered OK".format(i))

    print(">>> Phase III: Start monitor and worker threads")
    global_stats = GlobalStats(total_threads)

    monitor_th = threading.Thread(target=monitor_thread, args=(global_stats,), daemon=True)
    monitor_th.start()

    worker_threads = []
    for i in range(total_threads):
        if OPERATION_MODE == "batch_put":
            target = batch_put_worker
        elif OPERATION_MODE == "batch_get":
            target = batch_get_worker
        elif OPERATION_MODE == "mixed":
            target = batch_put_worker if i % 2 == 0 else batch_get_worker
        else:
            target = batch_put_worker

        th = threading.Thread(
            target=target,
            args=(i, stores[i], registered_ptrs[i], BLOCK_SIZE, BATCH_SIZE, global_stats),
            daemon=True
        )
        th.start()
        worker_threads.append(th)

    print("    Started {} worker threads + 1 monitor thread".format(total_threads))

    try:
        start_time = time.time()
        while time.time() - start_time < TEST_DURATION:
            if stop_event.is_set():
                print("    Detected error, exiting early")
                break
            time.sleep(0.1)
        print("    Test duration reached, stopping all threads...")
        stop_event.set()
    except KeyboardInterrupt:
        print("    Keyboard interrupt, stopping...")
        stop_event.set()

    monitor_th.join(timeout=2)
    for th in worker_threads:
        th.join(timeout=2)

    print_final_report(global_stats, args)

    for store, ptr in zip(stores, registered_ptrs):
        try:
            store.unregister_buffer(ptr)
        except Exception:
            pass

    del stores
    del registered_ptrs
    del buf
    del mm
    del global_stats
    gc.collect()
    print(">>> Test complete")


if __name__ == "__main__":
    args = parse_args()
    try:
        run_stress_test(args)
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print("Exception: {}".format(e))
    finally:
        stop_event.set()