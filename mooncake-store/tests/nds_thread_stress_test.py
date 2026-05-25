import argparse
import gc
import mmap
import numpy as np
import time
import threading
import os
import subprocess
import shutil
import socket
import tempfile
import urllib.request
import urllib.error
import logging
from mooncake.store import MooncakeDistributedStore
import mooncake.store

GB = 1024**3
MB = 1024**2

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

BLOCK_SIZE = 128 * 1024
BATCH_SIZE = 128
NUM_THREADS = 8
TEST_DURATION = 30
MONITOR_INTERVAL = 1

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_tcp_port(host, port, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("Timed out waiting for TCP port {}:{}".format(host, port))


def wait_for_metadata_server(url, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url + "?key=nds_test_probe", timeout=1.0):
                return True
        except urllib.error.HTTPError as exc:
            if exc.code in (200, 400, 404):
                return True
        except urllib.error.URLError:
            time.sleep(0.1)
    raise RuntimeError("Timed out waiting for metadata server {}".format(url))


def resolve_master_binary(master_binary_arg=""):
    if master_binary_arg and os.path.isfile(master_binary_arg):
        return master_binary_arg
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    build_dir = os.environ.get("MOONCAKE_BUILD_DIR", "build")
    local_binary = os.path.join(repo_root, build_dir, "mooncake-store", "src", "mooncake_master")
    if os.path.isfile(local_binary):
        return local_binary
    binary = shutil.which("mooncake_master")
    if binary:
        return binary
    raise FileNotFoundError(
        "Cannot find mooncake_master. Build it first or install it into PATH.")


def start_master(args):
    master_binary = resolve_master_binary(args.master_binary)
    rpc_port = find_free_port()
    http_port = find_free_port()
    metrics_port = find_free_port()

    master_log_fd, master_log_path = tempfile.mkstemp(
        prefix="nds_thread_stress_master-", suffix=".log")
    os.close(master_log_fd)
    master_log_file = open(master_log_path, "w", encoding="utf-8")

    master_data_dir = tempfile.mkdtemp(prefix="nds_thread_stress_data-")
    cmd = [
        master_binary,
        "--use_od=true",
        "--root_fs_dir={}".format(master_data_dir),
        "--cluster_id=nds_thread_stress",
        "--enable_http_metadata_server=true",
        "--rpc_address=127.0.0.1",
        "--rpc_port={}".format(rpc_port),
        "--http_metadata_server_host=127.0.0.1",
        "--http_metadata_server_port={}".format(http_port),
        "--metrics_port={}".format(metrics_port),
        "--default_kv_lease_ttl=500",
        "--rpc_thread_num={}".format(args.num_threads * 2),
    ]

    print(">>> Starting master server...")
    print("    Command: {}".format(" ".join(cmd)))
    master_proc = subprocess.Popen(
        cmd,
        stdout=master_log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )

    metadata_url = "http://127.0.0.1:{}/metadata".format(http_port)
    try:
        wait_for_tcp_port("127.0.0.1", rpc_port)
        wait_for_metadata_server(metadata_url)
    except Exception as e:
        print("ERROR: Master failed to start: {}".format(e))
        try:
            with open(master_log_path, "r") as f:
                print("Master log:\n{}".format(f.read()[:2000]))
        except Exception:
            pass
        stop_master(master_proc, master_log_file, master_log_path)
        raise

    print("    Master started - RPC: 127.0.0.1:{}, Metadata: {}".format(
        rpc_port, metadata_url))
    return master_proc, master_log_file, master_log_path, rpc_port, http_port, master_data_dir


def stop_master(master_proc, master_log_file, master_log_path):
    print(">>> Stopping master server...")
    if master_proc and master_proc.poll() is None:
        master_proc.terminate()
        try:
            master_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            master_proc.kill()
            master_proc.wait(timeout=2)
    if master_log_file and not master_log_file.closed:
        master_log_file.close()
    if master_log_path and os.path.exists(master_log_path):
        try:
            os.remove(master_log_path)
        except Exception:
            pass
    print("    Master stopped")


def generate_batch_keys(entity_id, batch_seq, batch_size):
    return ["t{}_b{}_k{}".format(entity_id, batch_seq, i)
            for i in range(batch_size)]


class ThreadStats:
    def __init__(self, num_threads):
        self.lock = threading.Lock()
        self.start_time = time.time()
        self.bw_start_time = time.time()
        self.thread_stats = {}
        for i in range(num_threads):
            self.thread_stats[i] = {
                "io_count": 0, "total_bytes": 0, "total_latency": 0.0,
                "success_ops": 0, "error_count": 0,
                "all_io_count": 0, "all_total_bytes": 0,
                "all_total_latency": 0.0, "all_success_ops": 0, "all_error_count": 0,
            }

    def add_batch_stats(self, thread_idx, success, bytes_transferred, latency):
        with self.lock:
            s = self.thread_stats[thread_idx]
            s["io_count"] += 1
            s["all_io_count"] += 1
            s["total_bytes"] += bytes_transferred
            s["all_total_bytes"] += bytes_transferred
            s["total_latency"] += latency
            s["all_total_latency"] += latency
            if success:
                s["success_ops"] += 1
                s["all_success_ops"] += 1
            else:
                s["error_count"] += 1
                s["all_error_count"] += 1

    def get_all_ops(self):
        with self.lock:
            return sum(s["all_io_count"] for s in self.thread_stats.values())

    def get_all_bytes(self):
        with self.lock:
            return sum(s["all_total_bytes"] for s in self.thread_stats.values())

    def get_all_errors(self):
        with self.lock:
            return sum(s["all_error_count"] for s in self.thread_stats.values())

    def get_all_avg_latency(self):
        with self.lock:
            total_ops = sum(s["all_io_count"] for s in self.thread_stats.values())
            total_lat = sum(s["all_total_latency"] for s in self.thread_stats.values())
            if total_ops == 0:
                return 0
            return total_lat / total_ops

    def snapshot_and_reset(self):
        with self.lock:
            elapsed = time.time() - self.bw_start_time
            total_bytes = sum(s["total_bytes"] for s in self.thread_stats.values())
            total_ops = sum(s["io_count"] for s in self.thread_stats.values())
            total_lat = sum(s["total_latency"] for s in self.thread_stats.values())
            total_errors = sum(s["error_count"] for s in self.thread_stats.values())
            avg_lat = total_lat / total_ops if total_ops > 0 else 0
            bw = total_bytes / elapsed / GB if elapsed > 0 else 0
            self.bw_start_time = time.time()
            for s in self.thread_stats.values():
                s["io_count"] = 0
                s["total_bytes"] = 0
                s["total_latency"] = 0.0
                s["success_ops"] = 0
                s["error_count"] = 0
            return bw, avg_lat, total_ops, total_errors


def worker_thread(thread_idx, operation_mode, block_size, batch_size,
                   store, buf_ptr, thread_offset, test_duration,
                   thread_stats):
    batch_seq = 0
    total_batch_bytes = batch_size * block_size
    thread_buf_ptr = buf_ptr + thread_offset

    deadline = time.time() + test_duration

    while time.time() < deadline:
        try:
            batch_keys = generate_batch_keys(thread_idx, batch_seq, batch_size)

            buffer_ptrs = []
            sizes = []
            for i in range(len(batch_keys)):
                offset = i * block_size
                buffer_ptrs.append(thread_buf_ptr + offset)
                sizes.append(block_size)

            start_time = time.time()
            if operation_mode == "batch_put":
                ret_codes = store.batch_put_from(batch_keys, buffer_ptrs, sizes)
                all_success = all(rc == 0 for rc in ret_codes)
            elif operation_mode == "batch_get":
                ret_codes = store.batch_get_into(batch_keys, buffer_ptrs, sizes)
                all_success = all(rc > 0 for rc in ret_codes)
            latency = time.time() - start_time

            batch_seq += 1

            thread_stats.add_batch_stats(thread_idx, all_success,
                                         total_batch_bytes, latency)

            if not all_success:
                failed_count = sum(1 for rc in ret_codes
                                   if (operation_mode == "batch_put" and rc != 0)
                                   or (operation_mode == "batch_get" and rc <= 0))
                if failed_count > 3:
                    logger.warning("Thread {} {}: {} keys failed".format(
                        thread_idx, operation_mode, failed_count))
        except Exception as e:
            logger.error("Thread {} exception: {}".format(thread_idx, e))
            thread_stats.add_batch_stats(thread_idx, False, 0, 0)
            break


def print_final_report(thread_stats, args):
    print("\n" + "=" * 80)
    print("NDS THREAD STRESS TEST - FINAL REPORT".center(80))
    print("=" * 80)
    elapsed = time.time() - thread_stats.start_time
    print("Test duration:       {:.2f}s".format(elapsed))
    print("Operation mode:      {}".format(args.operation_mode))
    print("Batch size:          {}".format(args.batch_size))
    print("Block size:          {} ({:.2f} MB)".format(args.block_size, args.block_size / MB))
    print("Num threads:         {}".format(args.num_threads))
    print("-" * 80)

    total_bytes = thread_stats.get_all_bytes()
    total_errors = thread_stats.get_all_errors()
    total_ops = thread_stats.get_all_ops()
    avg_lat = thread_stats.get_all_avg_latency()

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
    parser = argparse.ArgumentParser(description="NDS Thread Stress Test - Single Client, Multiple Threads")
    parser.add_argument("--operation-mode", type=str, default="batch_put",
                        choices=["batch_put", "batch_get", "mixed"],
                        help="Operation mode: batch_put, batch_get, or mixed")
    parser.add_argument("--block-size", type=int, default=128 * 1024,
                        help="Size of a single block in bytes")
    parser.add_argument("--batch-size", type=int, default=128,
                        help="Number of keys per batch operation")
    parser.add_argument("--num-threads", type=int, default=8,
                        help="Number of threads sharing one Client")
    parser.add_argument("--duration", type=int, default=30,
                        help="Test duration in seconds")
    parser.add_argument("--monitor-interval", type=int, default=1,
                        help="Monitor report interval in seconds")

    parser.add_argument("--protocol", type=str, default="tcp",
                        help="Transfer protocol")
    parser.add_argument("--device-name", type=str, default="",
                        help="RDMA device name (empty for TCP)")
    parser.add_argument("--local-hostname", type=str, default="127.0.0.1:0",
                        help="Local hostname (port 0 = auto-detect)")
    parser.add_argument("--global-segment-size", type=int, default=512,
                        help="Global segment size in MB")
    parser.add_argument("--master-binary", type=str, default="",
                        help="Path to mooncake_master binary (auto-detect if empty)")
    return parser.parse_args()


def run_thread_stress_test(args):
    operation_mode = args.operation_mode
    block_size = args.block_size
    batch_size = args.batch_size
    num_threads = args.num_threads
    test_duration = args.duration
    monitor_interval = args.monitor_interval

    per_thread_buffer_size = batch_size * block_size
    total_buffer_size = per_thread_buffer_size * num_threads

    print("=" * 80)
    print("NDS THREAD STRESS TEST (Single Process, Single Client)".center(80))
    print("=" * 80)
    print("Operation mode:      {}".format(operation_mode))
    print("Batch size:          {}".format(batch_size))
    print("Block size:          {} ({:.2f} MB)".format(block_size, block_size / MB))
    print("Num threads:         {}".format(num_threads))
    print("Per-thread buffer:   {} ({:.2f} MB)".format(per_thread_buffer_size, per_thread_buffer_size / MB))
    print("Total buffer:        {} ({:.2f} MB)".format(total_buffer_size, total_buffer_size / MB))
    print("Test duration:       {}s".format(test_duration))
    print("Protocol:            {}".format(args.protocol))
    print("=" * 80)

    master_proc = None
    master_log_file = None
    master_log_path = None
    master_data_dir = None

    thread_stats = ThreadStats(num_threads)

    try:
        master_proc, master_log_file, master_log_path, rpc_port, http_port, master_data_dir = start_master(args)
        metadata_url = "http://127.0.0.1:{}/metadata".format(http_port)
        master_addr = "127.0.0.1:{}".format(rpc_port)

        global_segment_size = args.global_segment_size * MB

        print(">>> Phase I: Setup single Client with NDS init memory = shared buffer")
        mooncake.store.init_glog()
        mooncake.store.set_vlog_level(0)
        mooncake.store.set_log_to_stderr(True)

        mm = mmap.mmap(-1, total_buffer_size, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
        buf = np.frombuffer(mm, dtype=np.uint8, count=total_buffer_size)
        buf_ptr = buf.ctypes.data

        pattern = (np.arange(block_size, dtype=np.uint32) % 251).astype(np.uint8)
        for t in range(num_threads):
            offset = t * per_thread_buffer_size
            buf[offset:offset + per_thread_buffer_size] = np.tile(pattern, batch_size)

        local_hostname = args.local_hostname
        if local_hostname.endswith(":0"):
            local_hostname = local_hostname[:-2] + ":{}".format(find_free_port())

        store = MooncakeDistributedStore()
        retcode = store.setup(
            local_hostname=local_hostname,
            metadata_server=metadata_url,
            global_segment_size=global_segment_size,
            local_buffer_size=total_buffer_size,
            protocol=args.protocol,
            rdma_devices=args.device_name,
            master_server_addr=master_addr,
            nds_mem_addr=buf_ptr,
            nds_mem_size=total_buffer_size,
        )
        if retcode:
            print("ERROR: Client setup failed, retcode={}".format(retcode))
            mm.close()
            return

        print("    Client setup OK (NDS init memory = buffer at 0x{:x}, size={})".format(
            buf_ptr, total_buffer_size))

        retcode = store.register_buffer(buf_ptr, total_buffer_size)
        if retcode:
            print("ERROR: register_buffer failed, retcode={}".format(retcode))
            mm.close()
            return

        print("    register_buffer OK")

        print(">>> Phase II: Spawn {} threads sharing one Client".format(num_threads))
        threads = []
        for i in range(num_threads):
            thread_mode = operation_mode
            if operation_mode == "mixed":
                thread_mode = "batch_put" if i % 2 == 0 else "batch_get"

            thread_offset = i * per_thread_buffer_size
            t = threading.Thread(
                target=worker_thread,
                args=(i, thread_mode, block_size, batch_size,
                      store, buf_ptr, thread_offset, test_duration,
                      thread_stats),
                daemon=True,
            )
            t.start()
            threads.append(t)

        print(">>> Phase III: Running stress test for {}s...".format(test_duration))
        start_time = time.time()
        while time.time() - start_time < test_duration:
            time.sleep(monitor_interval)

            elapsed = time.time() - thread_stats.start_time
            bw, avg_lat, total_ops, total_errors = thread_stats.snapshot_and_reset()
            error_rate = total_errors / total_ops * 100 if total_ops > 0 else 0

            print("[Monitor] {:6.1f}s - BW: {:5.2f} GB/s, AvgLat: {:.6f}s, "
                  "Errors: {:3d}, ErrorRate: {:.1f}%".format(
                      elapsed, bw, avg_lat, total_errors, error_rate))

            alive = sum(1 for t in threads if t.is_alive())
            if alive == 0:
                print("    All threads died, stopping")
                break

        print("    Test duration reached, waiting for threads to finish...")
        for t in threads:
            t.join(timeout=2)

        try:
            store.unregister_buffer(buf_ptr)
        except Exception:
            pass
        mm.close()

        print_final_report(thread_stats, args)

    except Exception as e:
        print("ERROR: {}".format(e))
    finally:
        print(">>> Cleanup")
        stop_master(master_proc, master_log_file, master_log_path)
        if master_data_dir and os.path.exists(master_data_dir):
            shutil.rmtree(master_data_dir, ignore_errors=True)
        gc.collect()
        print(">>> Test complete")


if __name__ == "__main__":
    args = parse_args()
    try:
        run_thread_stress_test(args)
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print("Exception: {}".format(e))