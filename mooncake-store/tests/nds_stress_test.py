import argparse
import gc
import mmap
import numpy as np
import time
import multiprocessing
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

MSG_BATCH_RESULT = "batch_result"
MSG_SETUP_OK = "setup_ok"
MSG_SETUP_FAIL = "setup_fail"
MSG_WORKER_DONE = "worker_done"
MSG_WORKER_ERROR = "worker_error"


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_tcp_port(host, port, timeout=20.0, master_proc=None):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if master_proc is not None and master_proc.poll() is not None:
            raise RuntimeError(
                "Master process exited unexpectedly (code={}) while waiting for TCP port {}:{}".format(
                    master_proc.returncode, host, port))
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("Timed out waiting for TCP port {}:{}".format(host, port))


_no_proxy_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def wait_for_metadata_server(url, timeout=20.0, master_proc=None):
    deadline = time.time() + timeout
    last_exc = None
    while time.time() < deadline:
        if master_proc is not None and master_proc.poll() is not None:
            raise RuntimeError(
                "Master process exited unexpectedly (code={}) while waiting for metadata server {}".format(
                    master_proc.returncode, url))
        try:
            with _no_proxy_opener.open(url + "?key=nds_test_probe", timeout=1.0):
                return True
        except urllib.error.HTTPError as exc:
            if exc.code in (200, 400, 404):
                return True
            last_exc = exc
            time.sleep(0.1)
        except urllib.error.URLError as exc:
            last_exc = exc
            time.sleep(0.1)
        except Exception as exc:
            last_exc = exc
            time.sleep(0.1)
    raise RuntimeError("Timed out waiting for metadata server {}: last error: {}".format(url, last_exc))


def resolve_master_binary(master_binary_arg=""):
    if master_binary_arg and os.path.isfile(master_binary_arg):
        return master_binary_arg
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    build_dir = os.environ.get("MOONCAKE_BUILD_DIR", "build")
    for ext in ["", ".exe"]:
        local_binary = os.path.join(repo_root, build_dir, "mooncake-store", "src", "mooncake_master" + ext)
        if os.path.isfile(local_binary):
            return local_binary
    binary = shutil.which("mooncake_master")
    if binary:
        return binary
    raise FileNotFoundError(
        "Cannot find mooncake_master. Build it first or install it into PATH.")


def start_master(args):
    os.environ["MC_TCP_BIND_ADDRESS"] = "127.0.0.1"
    master_binary = resolve_master_binary(args.master_binary)
    rpc_port = find_free_port()
    http_port = find_free_port()
    metrics_port = find_free_port()

    master_log_fd, master_log_path = tempfile.mkstemp(
        prefix="nds_stress_master-", suffix=".log")
    os.close(master_log_fd)
    master_log_file = open(master_log_path, "w", encoding="utf-8")

    cmd = [
        master_binary,
        "--use_od=true",
        "--nsid=1",
        "--cluster_id=nds_stress",
        "--enable_http_metadata_server=true",
        "--rpc_address=127.0.0.1",
        "--rpc_port={}".format(rpc_port),
        "--http_metadata_server_host=127.0.0.1",
        "--http_metadata_server_port={}".format(http_port),
        "--metrics_port={}".format(metrics_port),
        "--default_kv_lease_ttl=500",
        "--rpc_thread_num={}".format(args.num_workers * 2),
    ]

    print(">>> Starting master server...")
    print("    Command: {}".format(" ".join(cmd)))
    master_proc = subprocess.Popen(
        cmd,
        stdout=master_log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )

    time.sleep(0.5)
    if master_proc.poll() is not None:
        master_log_file.flush()
        log_content = ""
        try:
            with open(master_log_path, "r") as f:
                log_content = f.read()
        except Exception:
            pass
        stop_master(master_proc, master_log_file, master_log_path)
        raise RuntimeError(
            "Master process exited immediately with code {}. Log:\n{}".format(
                master_proc.returncode, log_content[:3000]))

    metadata_url = "http://127.0.0.1:{}/metadata".format(http_port)
    try:
        wait_for_tcp_port("127.0.0.1", rpc_port, master_proc=master_proc)
        wait_for_metadata_server(metadata_url, master_proc=master_proc)
    except Exception as e:
        print("ERROR: Master failed to start: {}".format(e))
        master_log_file.flush()
        try:
            with open(master_log_path, "r") as f:
                print("Master log:\n{}".format(f.read()[:3000]))
        except Exception:
            pass
        stop_master(master_proc, master_log_file, master_log_path)
        raise

    print("    Master started - RPC: 127.0.0.1:{}, Metadata: {}".format(
        rpc_port, metadata_url))
    return master_proc, master_log_file, master_log_path, rpc_port, http_port


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


def generate_batch_keys(entity_id, slot_idx, batch_size):
    return ["w{}_s{}_k{}".format(entity_id, slot_idx, i)
            for i in range(batch_size)]


def _remove_slot(store, worker_idx, slot):
    regex = "^w{}_s{}_k\\d+$".format(worker_idx, slot)
    try:
        removed = store.remove_by_regex(regex, force=True)
        logger.debug("Worker {} removed {} keys from slot {}".format(
            worker_idx, removed, slot))
        return removed
    except Exception as e:
        logger.warning("Worker {} remove slot {} failed: {}".format(
            worker_idx, slot, e))
        return 0


def worker_process(worker_idx, operation_mode, block_size, batch_size,
                   metadata_url, master_addr, global_segment_size,
                   protocol, device_name, local_hostname_base, test_duration,
                   stats_queue, stop_event, core_id, depth, eviction_window):
    buffer_size = batch_size * block_size

    if core_id >= 0:
        try:
            os.sched_setaffinity(0, {core_id})
            logger.info("Worker {} bound to core {}".format(worker_idx, core_id))
        except Exception as e:
            logger.warning("Worker {} failed to bind core {}: {}".format(
                worker_idx, core_id, e))

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
        local_hostname=local_hostname,
        metadata_server=metadata_url,
        global_segment_size=global_segment_size,
        local_buffer_size=0,
        protocol=protocol,
        rdma_devices=device_name,
        master_server_addr=master_addr,
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

    num_slots = depth if depth > 0 else 1
    slot_keys = [generate_batch_keys(worker_idx, s, batch_size)
                 for s in range(num_slots)]
    alive_slots = {}
    alive_order = []

    iteration = 0
    total_batch_bytes = batch_size * block_size

    try:
        while not stop_event.is_set():
            try:
                if depth > 0:
                    slot = iteration % depth
                    batch_keys = slot_keys[slot]
                else:
                    slot = iteration
                    batch_keys = generate_batch_keys(
                        worker_idx, iteration, batch_size)

                buffer_ptrs = []
                sizes = []
                for i in range(len(batch_keys)):
                    offset = i * block_size
                    buffer_ptrs.append(buf_ptr + offset)
                    sizes.append(block_size)

                if operation_mode == "batch_put":
                    if slot in alive_slots:
                        _remove_slot(store, worker_idx, slot)
                        alive_order.remove(slot)
                        del alive_slots[slot]

                start_time = time.time()
                if operation_mode == "batch_put":
                    ret_codes = store.batch_put_from(batch_keys, buffer_ptrs, sizes)
                    all_success = all(rc == 0 for rc in ret_codes)
                    if all_success:
                        alive_slots[slot] = iteration
                        alive_order.append(slot)
                elif operation_mode == "batch_get":
                    ret_codes = store.batch_get_into(batch_keys, buffer_ptrs, sizes)
                    all_success = all(rc > 0 for rc in ret_codes)
                latency = time.time() - start_time

                if operation_mode == "batch_put" and eviction_window > 0:
                    while len(alive_slots) > eviction_window:
                        oldest_slot = alive_order[0]
                        _remove_slot(store, worker_idx, oldest_slot)
                        alive_order.pop(0)
                        del alive_slots[oldest_slot]

                iteration += 1

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
        for slot in list(alive_slots.keys()):
            try:
                store.remove_by_regex(
                    "^w{}_s{}_k\\d+$".format(worker_idx, slot), force=True)
            except Exception:
                pass
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
        self.peak_bw = 0.0
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
        # Accumulate time across intervals with zero data to avoid
        # burst-driven bandwidth spikes. When no bytes were transferred
        # in this interval, keep the time window growing instead of
        # reporting 0 bandwidth. When bytes arrive, divide by the full
        # accumulated wall-clock span for a stable measurement.
        total_bytes = sum(s["total_bytes"] for s in self.worker_stats.values())
        total_ops = sum(s["io_count"] for s in self.worker_stats.values())
        total_lat = sum(s["total_latency"] for s in self.worker_stats.values())
        total_errors = sum(s["error_count"] for s in self.worker_stats.values())
        avg_lat = total_lat / total_ops if total_ops > 0 else 0

        elapsed = time.time() - self.bw_start_time

        if total_bytes > 0:
            bw = total_bytes / elapsed / GB if elapsed > 0 else 0
            if bw > self.peak_bw:
                self.peak_bw = bw
            self.bw_start_time = time.time()
            for s in self.worker_stats.values():
                s["io_count"] = 0
                s["total_bytes"] = 0
                s["total_latency"] = 0.0
                s["success_ops"] = 0
                s["error_count"] = 0
        else:
            bw = 0.0

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
    print("Depth:               {}".format(args.depth))
    print("Eviction window:     {}".format(args.eviction_window))
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
    print("Peak bandwidth:      {:5.2f} GB/s".format(global_stats.peak_bw))
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
                        help="RDMA device name(s), comma-separated for multiple devices "
                             "(empty for TCP). Workers round-robin across devices.")
    parser.add_argument("--local-hostname", type=str, default="127.0.0.1:0",
                        help="Local hostname (port 0 = auto-detect)")
    parser.add_argument("--global-segment-size", type=int, default=4096,
                        help="Global segment size in MB")
    parser.add_argument("--depth", type=int, default=1,
                        help="Ring buffer depth: number of key batches to cycle through. "
                             "depth=1 means reuse the same batch keys every iteration "
                             "(remove-before-put). depth=0 means infinite unique keys "
                             "(no ring buffer). Each worker's keys are independent "
                             "(w{worker}_s{slot}_k{idx}).")
    parser.add_argument("--eviction-window", type=int, default=1,
                        help="Max number of alive batches in memory at any time. "
                             "When exceeded, oldest batch is removed (master metadata + "
                             "shared memory pool + NDS storage). eviction_window=0 means "
                             "no eviction (memory will fill up). Must be <= depth when "
                             "depth>0, or any positive value when depth=0.")
    parser.add_argument("--core-bind-start", type=int, default=5,
                        help="Start CPU core for binding (-1 to disable)")
    parser.add_argument("--master-binary", type=str, default="",
                        help="Path to mooncake_master binary (auto-detect if empty)")
    return parser.parse_args()


def parse_device_names(device_name_arg):
    if not device_name_arg:
        return []
    devices = [d.strip() for d in device_name_arg.split(",") if d.strip()]
    return devices


def run_stress_test(args):
    operation_mode = args.operation_mode
    block_size = args.block_size
    batch_size = args.batch_size
    num_workers = args.num_workers
    test_duration = args.duration
    monitor_interval = args.monitor_interval
    device_list = parse_device_names(args.device_name)
    depth = args.depth
    eviction_window = args.eviction_window

    print("=" * 80)
    print("NDS STRESS TEST (Multi-Process)".center(80))
    print("=" * 80)
    print("Operation mode:      {}".format(operation_mode))
    print("Batch size:          {}".format(batch_size))
    print("Block size:          {} ({:.2f} MB)".format(block_size, block_size / MB))
    print("Num workers:         {}".format(num_workers))
    print("Test duration:       {}s".format(test_duration))
    print("Protocol:            {}".format(args.protocol))
    print("Depth:               {} (ring buffer slots)".format(depth))
    print("Eviction window:     {} (max alive batches)".format(eviction_window))
    if depth > 0:
        alive_mb = min(depth, eviction_window) * batch_size * block_size / MB if eviction_window > 0 else "infinite"
        print("Key space per worker: {} x {} = {} keys".format(
            depth, batch_size, depth * batch_size))
        print("Peak mem per worker: {} batches x {} blocks = {:.2f} MB".format(
            eviction_window if eviction_window > 0 else depth,
            batch_size,
            alive_mb if isinstance(alive_mb, float) else float("inf")))
    else:
        print("Key space:           infinite (slot=iteration)")
        if eviction_window > 0:
            print("Peak mem per worker: {} batches x {} blocks = {:.2f} MB".format(
                eviction_window, batch_size,
                eviction_window * batch_size * block_size / MB))
        else:
            print("Peak mem per worker: infinite (no eviction)")
    if device_list:
        print("RDMA devices:        {} (round-robin)".format(", ".join(device_list)))
    if args.core_bind_start >= 0:
        print("Core binding:        from core {} (sequential)".format(args.core_bind_start))
    print("=" * 80)

    master_proc = None
    master_log_file = None
    master_log_path = None

    stop_event = multiprocessing.Event()
    stats_queue = multiprocessing.Queue()
    workers = []
    global_stats = GlobalStats(num_workers)

    try:
        master_proc, master_log_file, master_log_path, rpc_port, http_port = start_master(args)
        metadata_url = "http://127.0.0.1:{}/metadata".format(http_port)
        master_addr = "127.0.0.1:{}".format(rpc_port)

        global_segment_size = args.global_segment_size * MB

        print(">>> Phase I: Spawn {} worker processes (each with own NDS)".format(num_workers))
        for i in range(num_workers):
            worker_mode = operation_mode
            if operation_mode == "mixed":
                worker_mode = "batch_put" if i % 2 == 0 else "batch_get"

            if device_list:
                worker_device = device_list[i % len(device_list)]
            else:
                worker_device = args.device_name

            if args.core_bind_start >= 0:
                core_id = args.core_bind_start + i
            else:
                core_id = -1

            p = multiprocessing.Process(
                target=worker_process,
                args=(i, worker_mode, block_size, batch_size,
                      metadata_url, master_addr,
                      global_segment_size,
                      args.protocol, worker_device,
                      args.local_hostname, test_duration,
                      stats_queue, stop_event, core_id,
                      depth, eviction_window),
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