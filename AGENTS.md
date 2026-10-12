# Mooncake Development Guide

Mooncake is a KVCache-centric disaggregated architecture for LLM serving. Core components: Transfer Engine (data transfer), Store (distributed KVCache), P2P Store (peer-to-peer objects), EP (elastic expert parallelism).

## Startup Workflow

1. Read this file for rules and conventions.
2. Read `feature_list.json` to find the active feature and its status.
3. Read `progress.md` for what was done last session.
4. Read `session-handoff.md` for blockers and next-step hints.
5. Run `./init.sh` to verify the environment (build, format, metadata server).
6. Work on ONE feature at a time. Do not start a new feature until the current one meets its done criteria.
7. Before claiming a feature is done, record evidence in `progress.md`: passing test output, format check output, and any manual verification.

## Definition of Done

A feature is done when:
- Code compiles without errors (`cmake .. && make -j` succeeds).
- `scripts/code_format.sh --check` passes (no formatting violations).
- All existing and new unit/integration tests pass.
- Evidence is recorded in `progress.md` with actual command output.
- `feature_list.json` status is updated to `done` with evidence description.

## Scope Boundary

- Do not modify components outside the active feature's scope without explicit instruction.
- Do not refactor unrelated code, add tangential improvements, or expand the feature beyond its description.
- If a change affects multiple features, stop and document the cross-cutting concern in `progress.md` before proceeding.

## End-of-Session Procedure

Before ending a session:
1. Run `scripts/code_format.sh --check` and record the result.
2. Run applicable tests and record pass/fail output.
3. Update `progress.md` with evidence, blockers, and decisions.
4. Update `feature_list.json` status for the active feature.
5. Write `session-handoff.md` with: current objective, verification evidence, files changed, blockers, and recommended next step.

## Harness State Files

- `feature_list.json` - Tracks all features, their dependencies, status, and evidence.
- `progress.md` - Session log: what's done, in-progress, blockers, decisions, evidence.
- `session-handoff.md` - End-of-session summary for the next agent to restart quickly.
- `init.sh` - Environment verification script (format check, build check, metadata server check).

## Build

Dependencies (requires sudo):
```bash
sudo bash dependencies.sh -y
```

Standard build:
```bash
mkdir build && cd build
cmake ..
make -j
sudo make install
```

Key CMake options (all OFF by default unless noted):
- `-DUSE_CUDA=ON` - NVIDIA GPU support (requires CUDA 12.1+)
- `-DUSE_ASCEND=ON` - Ascend NPU support
- `-DUSE_ETCD=ON` - Etcd metadata server (Go wrapper, default is HTTP)
- `-DWITH_EP=ON` - Elastic Expert Parallelism (requires CUDA, PyTorch)
- `-DWITH_STORE=ON` - Mooncake Store (default ON)
- `-DWITH_P2P_STORE=ON` - P2P Store
- `-DUSE_CXL=ON` - CXL transport
- `-DUSE_EFA=ON` - AWS EFA transport (requires libfabric)
- `-DUSE_MNNVL=ON` - Multi-Node NVLink
- `-DBUILD_UNIT_TESTS=ON` - Build C++ tests (default ON)
- `-DBUILD_EXAMPLES=ON` - Build examples (default ON)

Build Python wheel:
```bash
./scripts/build_wheel.sh
```
Environment vars: `PYTHON_VERSION=3.10`, `OUTPUT_DIR=dist`, `BUILD_DIR=build`.

## Test

C++ unit tests (requires `BUILD_UNIT_TESTS=ON`):
```bash
cd build
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata make test -j ARGS="-V"
```

Python tests (requires metadata server running):
```bash
mooncake_http_metadata_server --port 8080 &
./scripts/run_tests.sh
```

## Code Style

C++ formatting requires clang-format-20 (Google style with 4-space indent, 80-column limit):
```bash
./scripts/code_format.sh              # Format changed files vs origin/main
./scripts/code_format.sh --all        # Format all files
./scripts/code_format.sh --check      # Check only, no modifications
./scripts/code_format.sh -b origin/dev  # Compare against different branch
```

Pre-commit hooks: `pip install -r requirements-dev.txt && pre-commit install`

## Repository Structure

- `mooncake-transfer-engine/` - Core transport layer (TCP, RDMA, NVLink, EFA, CXL, Ascend)
- `mooncake-store/` - Distributed KVCache storage
- `mooncake-integration/` - Python bindings via pybind11 (`engine.so`, `store.so`)
- `mooncake-wheel/` - Python wheel packaging
- `mooncake-ep/` - Elastic Expert Parallelism CUDA extensions
- `mooncake-common/` - Shared utilities, etcd wrapper

## CI Notes

- CI runs on push to main, or when PR has `run-ci` label
- CI builds with: `-DUSE_HTTP=ON -DUSE_CXL=ON -DUSE_ETCD=ON -DSTORE_USE_ETCD=ON`
- Spell check uses `crate-ci/typos` (config: `.typos.toml`)
- Coverage via lcov, uploaded to Codecov

## Metadata Server

Start HTTP metadata server (simplest for testing):
```bash
mooncake_http_metadata_server --port 8080 &
```

Environment var: `MC_METADATA_SERVER=http://127.0.0.1:8080/metadata`

## Hardware Prerequisites

- RDMA: Mellanox OFED or similar RDMA driver
- CUDA: CUDA 12.1+ with GPUDirect Storage for `-DUSE_CUDA=ON`
- Ascend: CANN toolkit for `-DUSE_ASCEND=ON`
- Go 1.20+ required when `-DUSE_ETCD=ON` or `-DWITH_P2P_STORE=ON`