import argparse
import gc
import mmap
import numpy as np
import time
import multiprocessing
import sys
import os
import subprocess
import shutil
import socket
import tempfile
import urllib.request
import urllib.error
import logging
from collections import defaultdict
from mooncake.store import MooncakeDistributedStore
import mooncake.store

GB = 1024**3
MB = 1024**2

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

OPERATION_MODE = "batch_put"
BLOCK_SIZE = 128 * 1024
BATCH_SIZE = 128
NUM_WORKERS = 8
TEST_DURATION = 30
MONITOR_INTERVAL = 1

GLOBAL_SEGMENT_SIZE_MB = 3200
LOCAL_BUFFER_SIZE_MB = 512

MSG_BATCH_RESULT = "batch_result"
MSG_SETUP_OK = "setup_ok"
MSG_SETUP_FAIL = "setup_fail"
MSG_WORKER_DONE = "worker_done"
MSG_WORKER_ERROR = "worker_error"


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
        prefix="nds_stress_master-", suffix=".log")
    os.close(master_log_fd)
    master_log_file = open(master_log_path, "w", encoding="utf-8")

    master_data_dir = tempfile.mkdtemp(prefix="nds_stress_data-")
    cmd = [
        master_binary,
        "--use_od=true",
        "--root_fs_dir={}".format(master_data_dir),
        "--cluster_id=nds_stress",
        "--enable_http_metadata_server=true",
        "--rpc_address=127.0.0.1",
        "--rpc_port={}".format(rpc_port),
        "--http_metadata_server_host=127.0.0.1",
        "--http_metadata_server_port={}".format(http_port),
        "--metrics_port={}".format(metrics_port),
        "--default_kv_lease_ttl=500",
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
    return ["w{}_b{}_k{}".format(entity_id, batch_seq, i)
            for i in range(batch_size)]


def worker_process(worker_idx, operation_mode, block_size, batch_size,
                   metadata_url, master_addr, global_segment_size, local_buffer_size,
                   protocol, device_name, local_hostname_base, test_duration,
                   stats_queue, stop_event):
    buffer_size = batch_size * block_size

    mooncake.store.init_glog()
    mooncake.store.set_vlog_level(0)
    mooncake.store.set_log_to_stderr(True)

    mm = mmap.mmap(-1, buffer_size, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
    buf = np.frombuffer(mm, dtype=np.uint8, count=buffer_size)
    buf_ptr = buf.ctypes.data

    pattern = (np.arange(block_size, dtype=np.uint32) % 251).astype(np.uint8)
    full_pattern = np.tile(pattern, batch_size)
    buf[:] = full_pattern

    local_hostname = local_hostname_base
    if local_hostname.endswith(":0"):
        local_hostname = local_hostname[:-2] + ":{}".format(find_free_port())

    store = MooncakeDistributedStore()
    retcode = store.setup(
        local_hostname,
        metadata_url,
        global_segment_size,
        local_buffer_size,
        protocol,
        device_name,
        master_addr,
        nds_mem_addr=buf_ptr,
        nds_mem_size=buffer_size,
    )
    if retcode:
        stats_queue.put((MSG_SETUP_FAIL, worker_idx, retcode))
        mm.close()
        return

    retcode = store.register_buffer(buf_ptr, buffer_size)
    if retcode:
        stats_queue.put((MSG_SETUP_FAIL, worker_idx, retcode))
        mm.close()
        return

    stats_queue.put((MSG_SETUP_OK, worker_idx, 0))

    batch_seq = 0
    total_batch_bytes = batch_size * block_size

    try:
        while not stop_event.is_set():
            try:
                batch_keys = generate_batch_keys(worker_idx, batch_seq, batch_size)

                buffer_ptrs = []
                sizes = []
                for i in range(len(batch_keys)):
                    offset = i * block_size
                    buffer_ptrs.append(buf_ptr + offset)
                    sizes.append(block_size)

                start_time = time.time()
                if operation_mode == "batch_put":
                    ret_codes = store.batch_put_from(batch_keys, buffer_ptrs, sizes)
                    all_success = all(rc == 0 for rc in ret_codes)
                elif operation_mode == "batch_get":
                    put_codes = store.batch_put_from(batch_keys, buffer_ptrs, sizes)
                    put_ok = all(rc == 0 for rc in put_codes)
                    if put_ok:
                        ret_codes = store.batch_get_into(batch_keys, buffer_ptrs, sizes)
                        all_success = all(rc > 0 for rc in ret_codes)
                    else:
                        all_success = False
                        ret_codes = put_codes
                latency = time.time() - start_time

                batch_seq += 1

                stats_queue.put((MSG_BATCH_RESULT, worker_idx, all_success,
                                 total_batch_bytes, latency))

                if not all_success:
                    failed_count = sum(1 for rc in ret_codes
                                       if (operation_mode == "batch_put" and rc != 0)
                                       or (operation_mode == "batch_get" and rc <= 0))
                    if failed_count > 3:
                        logger.warning("Worker {} {}: {} keys failed".format(
                            worker_idx, operation_mode, failed_count))
            except Exception as e:
                logger.error("Worker {} exception: {}".format(worker_idx, e))
                stats_queue.put((MSG_BATCH_RESULT, worker_idx, False, 0, 0))
                break
    finally:
        try:
            store.unregister_buffer(buf_ptr)
        except Exception:
            pass
        mm.close()
        gc.collect()
        stats_queue.put((MSG_WORKER_DONE, worker_idx, 0))


class GlobalStats:
    def __init__(self, num_workers):
        self.start_time = time.time()
        self.bw_start_time = time.time()
        self.worker_stats = {}
        for i in range(num_workers):
            self.worker_stats[i] = {
                "io_count": 0, "total_bytes": 0, "total_latency": 0.0,
                "success_ops": 0, "error_count": 0,
                "all_io_count": 0, "all_total_bytes": 0,
                "all_total_latency": 0.0, "all_success_ops": 0, "all_error_count": 0,
            }

    def add_batch_stats(self, worker_idx, success, bytes_transferred, latency):
        s = self.worker_stats[worker_idx]
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
        return sum(s["all_io_count"] for s in self.worker_stats.values())

    def get_all_bytes(self):
        return sum(s["all_total_bytes"] for s in self.worker_stats.values())

    def get_all_errors(self):
        return sum(s["all_error_count"] for s in self.worker_stats.values())

    def get_all_avg_latency(self):
        total_ops = sum(s["all_io_count"] for s in self.worker_stats.values())
        total_lat = sum(s["all_total_latency"] for s in self.worker_stats.values())
        if total_ops == 0:
            return 0
        return total_lat / total_ops

    def snapshot_and_reset(self):
        elapsed = time.time() - self.bw_start_time
        total_bytes = sum(s["total_bytes"] for s in self.worker_stats.values())
        total_ops = sum(s["io_count"] for s in self.worker_stats.values())
        total_lat = sum(s["total_latency"] for s in self.worker_stats.values())
        total_errors = sum(s["error_count"] for s in self.worker_stats.values())
        avg_lat = total_lat / total_ops if total_ops > 0 else 0
        bw = total_bytes / elapsed / GB if elapsed > 0 else 0
        self.bw_start_time = time.time()
        for s in self.worker_stats.values():
            s["io_count"] = 0
            s["total_bytes"] = 0
            s["total_latency"] = 0.0
            s["success_ops"] = 0
            s["error_count"] = 0
        return bw, avg_lat, total_ops, total_errors

    def drain_queue(self, stats_queue):
        while not stats_queue.empty():
            try:
                msg = stats_queue.get_nowait()
            except Exception:
                break
            if msg[0] == MSG_BATCH_RESULT:
                _, worker_idx, success, total_size, latency = msg
                self.add_batch_stats(worker_idx, success, total_size, latency)


def print_final_report(global_stats, args):
    print("\n" + "=" * 80)
    print("NDS STRESS TEST - FINAL REPORT".center(80))
    print("=" * 80)
    elapsed = time.time() - global_stats.start_time
    print("Test duration:       {:.2f}s".format(elapsed))
    print("Operation mode:      {}".format(args.operation_mode))
    print("Batch size:          {}".format(args.batch_size))
    print("Block size:          {} ({:.2f} MB)".format(args.block_size, args.block_size / MB))
    print("Num workers:         {}".format(args.num_workers))
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
    parser.add_argument("--num-workers", type=int, default=8,
                        help="Number of worker processes (each gets its own NDS)")
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
    parser.add_argument("--global-segment-size", type=int, default=3200,
                        help="Global segment size in MB")
    parser.add_argument("--local-buffer-size", type=int, default=512,
                        help="Local buffer size in MB")
    parser.add_argument("--master-binary", type=str, default="",
                        help="Path to mooncake_master binary (auto-detect if empty)")
    return parser.parse_args()


def run_stress_test(args):
    operation_mode = args.operation_mode
    block_size = args.block_size
    batch_size = args.batch_size
    num_workers = args.num_workers
    test_duration = args.duration
    monitor_interval = args.monitor_interval

    print("=" * 80)
    print("NDS STRESS TEST (Multi-Process)".center(80))
    print("=" * 80)
    print("Operation mode:      {}".format(operation_mode))
    print("Batch size:          {}".format(batch_size))
    print("Block size:          {} ({:.2f} MB)".format(block_size, block_size / MB))
    print("Num workers:         {}".format(num_workers))
    print("Test duration:       {}s".format(test_duration))
    print("Protocol:            {}".format(args.protocol))
    print("=" * 80)

    master_proc = None
    master_log_file = None
    master_log_path = None
    master_data_dir = None

    stop_event = multiprocessing.Event()
    stats_queue = multiprocessing.Queue()
    workers = []
    global_stats = GlobalStats(num_workers)

    try:
        master_proc, master_log_file, master_log_path, rpc_port, http_port, master_data_dir = start_master(args)
        metadata_url = "http://127.0.0.1:{}/metadata".format(http_port)
        master_addr = "127.0.0.1:{}".format(rpc_port)

        global_segment_size = args.global_segment_size * MB
        local_buffer_size = args.local_buffer_size * MB

        print(">>> Phase I: Spawn {} worker processes (each with own NDS)".format(num_workers))
        for i in range(num_workers):
            worker_mode = operation_mode
            if operation_mode == "mixed":
                worker_mode = "batch_put" if i % 2 == 0 else "batch_get"

            p = multiprocessing.Process(
                target=worker_process,
                args=(i, worker_mode, block_size, batch_size,
                      metadata_url, master_addr,
                      global_segment_size, local_buffer_size,
                      args.protocol, args.device_name,
                      args.local_hostname, test_duration,
                      stats_queue, stop_event),
                daemon=True,
            )
            p.start()
            workers.append(p)

        setup_ok_count = 0
        setup_fail_count = 0
        setup_timeout = time.time() + 30
        while setup_ok_count + setup_fail_count < num_workers and time.time() < setup_timeout:
            global_stats.drain_queue(stats_queue)
            try:
                msg = stats_queue.get(timeout=0.5)
            except Exception:
                continue
            if msg[0] == MSG_SETUP_OK:
                setup_ok_count += 1
                print("    Worker {} setup OK".format(msg[1]))
            elif msg[0] == MSG_SETUP_FAIL:
                setup_fail_count += 1
                print("ERROR: Worker {} setup failed, retcode={}".format(msg[1], msg[2]))

        if setup_fail_count > 0:
            print("ERROR: {} workers failed setup. Stopping.".format(setup_fail_count))
            stop_event.set()
            return

        print("    All {} workers setup successfully".format(setup_ok_count))

        print(">>> Phase II: Running stress test for {}s...".format(test_duration))
        start_time = time.time()
        while time.time() - start_time < test_duration:
            if stop_event.is_set():
                print("    Detected stop signal, exiting early")
                break

            time.sleep(monitor_interval)
            global_stats.drain_queue(stats_queue)

            elapsed = time.time() - global_stats.start_time
            bw, avg_lat, total_ops, total_errors = global_stats.snapshot_and_reset()
            error_rate = total_errors / total_ops * 100 if total_ops > 0 else 0

            print("[Monitor] {:6.1f}s - BW: {:5.2f} GB/s, AvgLat: {:.6f}s, "
                  "Errors: {:3d}, ErrorRate: {:.1f}%".format(
                      elapsed, bw, avg_lat, total_errors, error_rate))

            alive = sum(1 for p in workers if p.is_alive())
            if alive == 0:
                print("    All workers died, stopping")
                break

        print("    Test duration reached, stopping all workers...")
        stop_event.set()

        for p in workers:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()

        global_stats.drain_queue(stats_queue)
        print_final_report(global_stats, args)

    except Exception as e:
        print("ERROR: {}".format(e))
    finally:
        print(">>> Cleanup")
        stop_event.set()
        for p in workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=2)
        stop_master(master_proc, master_log_file, master_log_path)
        if master_data_dir and os.path.exists(master_data_dir):
            shutil.rmtree(master_data_dir, ignore_errors=True)
        gc.collect()
        print(">>> Test complete")


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    args = parse_args()
    try:
        run_stress_test(args)
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print("Exception: {}".format(e))