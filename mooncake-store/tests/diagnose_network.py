import os
import socket
import subprocess
import shutil
import sys
import tempfile
import time
import urllib.request
import urllib.error


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def resolve_master_binary():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    build_dir = os.environ.get("MOONCAKE_BUILD_DIR", "build")
    for ext in ["", ".exe"]:
        local_binary = os.path.join(repo_root, build_dir, "mooncake-store", "src", "mooncake_master" + ext)
        if os.path.isfile(local_binary):
            return local_binary
    binary = shutil.which("mooncake_master")
    if binary:
        return binary
    print("ERROR: Cannot find mooncake_master binary")
    sys.exit(1)


def check_tcp_port(host, port, timeout=3.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError as e:
        print("  TCP connect to {}:{} failed: {}".format(host, port, e))
        return False


_no_proxy_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
_default_opener = urllib.request.build_opener()


def check_http_metadata(url, timeout=3.0, use_no_proxy=False):
    opener = _no_proxy_opener if use_no_proxy else _default_opener
    label = "NO-PROXY opener" if use_no_proxy else "DEFAULT opener (may use system proxy)"
    try:
        resp = opener.open(url + "?key=diag_probe", timeout=timeout)
        print("  {} -> HTTP {} {}".format(label, resp.status, resp.reason))
        return True
    except urllib.error.HTTPError as e:
        print("  {} -> HTTPError {} {}".format(label, e.code, e.reason))
        return e.code in (200, 400, 404)
    except urllib.error.URLError as e:
        print("  {} -> URLError: {}".format(label, e.reason))
        return False
    except Exception as e:
        print("  {} -> Exception: {}".format(label, e))
        return False


def check_env_proxy():
    print("=== Environment Proxy Settings ===")
    for var in ["http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "no_proxy", "NO_PROXY"]:
        val = os.environ.get(var, "")
        if val:
            print("  {} = {}".format(var, val))
        else:
            print("  {} = (not set)".format(var))
    print()


def main():
    print("=" * 60)
    print("Mooncake Master Network Diagnostic Script")
    print("=" * 60)
    print()

    check_env_proxy()

    master_binary = resolve_master_binary()
    print("Master binary: {}".format(master_binary))

    rpc_port = find_free_port()
    http_port = find_free_port()
    metrics_port = find_free_port()

    print("Ports: RPC={}, HTTP metadata={}, metrics={}".format(rpc_port, http_port, metrics_port))

    log_fd, log_path = tempfile.mkstemp(prefix="diag_master-", suffix=".log")
    os.close(log_fd)
    log_file = open(log_path, "w", encoding="utf-8")

    cmd = [
        master_binary,
        "--use_od=true",
        "--cluster_id=diag_test",
        "--enable_http_metadata_server=true",
        "--rpc_address=127.0.0.1",
        "--rpc_port={}".format(rpc_port),
        "--http_metadata_server_host=127.0.0.1",
        "--http_metadata_server_port={}".format(http_port),
        "--metrics_port={}".format(metrics_port),
    ]

    print("\nCommand: {}".format(" ".join(cmd)))
    proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT, text=True)

    print("Master PID: {}".format(proc.pid))
    time.sleep(1.0)

    if proc.poll() is not None:
        log_file.flush()
        with open(log_path, "r") as f:
            print("Master exited with code {}. Log:\n{}".format(proc.returncode, f.read()[:3000]))
        log_file.close()
        os.remove(log_path)
        sys.exit(1)

    print("\n=== Step 1: Check master process alive ===")
    alive = proc.poll() is None
    print("  Process alive: {}".format(alive))

    print("\n=== Step 2: Check RPC TCP port {} (with retry) ===".format(rpc_port))
    rpc_deadline = time.time() + 10.0
    tcp_ok = False
    while time.time() < rpc_deadline:
        if proc.poll() is not None:
            print("  Master process died during RPC port check (code={})".format(proc.returncode))
            break
        tcp_ok = check_tcp_port("127.0.0.1", rpc_port, timeout=1.0)
        if tcp_ok:
            break
        print("  ... RPC port not ready yet, waiting...")
        time.sleep(0.5)
    print("  RPC port reachable: {}".format(tcp_ok))

    print("\n=== Step 3: Check HTTP metadata port {} (TCP level) ===".format(http_port))
    http_tcp_ok = check_tcp_port("127.0.0.1", http_port)
    print("  HTTP port reachable (TCP): {}".format(http_tcp_ok))

    metadata_url = "http://127.0.0.1:{}/metadata".format(http_port)

    print("\n=== Step 4: HTTP request with DEFAULT opener (uses system proxy) ===")
    default_ok = check_http_metadata(metadata_url, use_no_proxy=False)

    print("\n=== Step 5: HTTP request with NO-PROXY opener (bypasses proxy) ===")
    noproxy_ok = check_http_metadata(metadata_url, use_no_proxy=True)

    print("\n=== Step 6: Raw socket HTTP request (no proxy possible) ===")
    try:
        s = socket.create_connection(("127.0.0.1", http_port), timeout=3.0)
        s.sendall(b"GET /metadata?key=diag_probe HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n")
        resp_raw = s.recv(4096).decode("utf-8", errors="replace")
        s.close()
        status_line = resp_raw.split("\r\n")[0] if resp_raw else "(empty)"
        print("  Raw socket -> {}".format(status_line))
        raw_ok = "200" in status_line or "400" in status_line or "404" in status_line
    except Exception as e:
        print("  Raw socket failed: {}".format(e))
        raw_ok = False

    print("\n=== Step 7: urllib proxy handler diagnostic ===")
    print("  Default opener handlers:")
    for h in _default_opener.handlers:
        print("    {}".format(type(h).__name__))
    print("  No-proxy opener handlers:")
    for h in _no_proxy_opener.handlers:
        print("    {}".format(type(h).__name__))

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("  Process alive:     {}".format(alive))
    print("  RPC TCP:           {}".format(tcp_ok))
    print("  HTTP TCP:          {}".format(http_tcp_ok))
    print("  HTTP DEFAULT:      {}".format(default_ok))
    print("  HTTP NO-PROXY:     {}".format(noproxy_ok))
    print("  HTTP RAW SOCKET:   {}".format(raw_ok))
    print()

    if not tcp_ok and not http_tcp_ok:
        print("CONCLUSION: Master process is alive but ports are unreachable.")
        print("  Likely cause: firewall blocking 127.0.0.1 or process bind failure.")
    elif tcp_ok and http_tcp_ok and not default_ok and noproxy_ok:
        print("CONCLUSION: Proxy issue confirmed!")
        print("  System proxy intercepts requests to 127.0.0.1.")
        print("  Fix: use no_proxy opener or set no_proxy=127.0.0.1,localhost")
    elif tcp_ok and http_tcp_ok and default_ok and noproxy_ok:
        print("CONCLUSION: Network OK, no proxy interference.")
        print("  If tests still fail, the issue is elsewhere.")
    elif tcp_ok and http_tcp_ok and not default_ok and not noproxy_ok:
        print("CONCLUSION: HTTP metadata server not responding correctly.")
        print("  TCP port is open but HTTP protocol fails at all levels.")
        print("  Likely cause: master HTTP server bug or bind issue.")
    else:
        print("CONCLUSION: Mixed results, see details above.")

    print("\n=== Master log (last 30 lines) ===")
    log_file.flush()
    try:
        with open(log_path, "r") as f:
            lines = f.readlines()
        for line in lines[-30:]:
            print("  {}".format(line.rstrip()))
    except Exception:
        print("  (could not read log)")

    print("\n=== Cleanup ===")
    print("  Log file preserved at: {}".format(log_path))
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
    log_file.close()
    print("Master stopped. Log NOT removed so you can inspect it.")


if __name__ == "__main__":
    main()