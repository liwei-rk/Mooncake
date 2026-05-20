# NDS Production Implementation Plan

## Context

NDS is a simple KV storage interface (`init`/`put`/`get`/`isExists`) defined in `mooncake-store/include/nds/nds_interface.h`. Currently only a mock implementation exists (`nds_mock.cpp`) that stores blocks as local files in `kv_data/`. The real NDS should use direct I/O (`O_DIRECT`) on NVMe SSD for production use.

The integration is currently half-done: only `NDS::put()` is called from `StorageBackend::StoreObject(slices, key)` at [storage_backend.cpp:310](mooncake-store/src/storage_backend.cpp#L310), and it's unconditional (no compile-time gate). `NDS::get()`, `NDS::init()`, `NDS::isExists()` are never called from production code. The `LoadObject` path still uses file I/O exclusively.

## Design Decisions

1. **NDS fields on StorageBackend are unconditional** — `bool`, `std::string`, `uint64_t` are small; only `.cpp` code has `#ifdef USE_NDS` guards
2. **NDS is compiled into `mooncake_store` directly** (not a separate library) — mock stays as standalone `libndsclient`
3. **When NDS is active, file-system eviction/Init is skipped** — same pattern as 3FS mode
4. **`PutToLocalFile` calls the slices overload** — eliminates code duplication without `#ifdef` in client_service.cpp
5. **O_DIRECT alignment handled inside NDS implementation** — callers are unaware of alignment

## Implementation Steps

### Step 1: Create `mooncake-store/src/nds/nds_production.cpp`

New file implementing the `NDS` namespace with O_DIRECT NVMe storage:

- `init(void* addr, uint64_t len)` — `addr` is the NVMe device/file path, `len` is capacity; opens with `O_RDWR | O_DIRECT`, allocates a `posix_memalign` bounce buffer for misaligned I/O
- `put(blockId, blockAddr, offset, len)` — absolute device offset = `blockId * kBlockSize + offset`; fast path for fully-aligned, read-modify-write via bounce buffer for misaligned
- `get(blockId, blockAddr, offset, len)` — same alignment handling as put
- `isExists(blockIds)` — maintains an in-memory `std::unordered_set<uint64_t>` of written block IDs (ephemeral cache use case, no crash recovery needed)
- `extern "C"` wrappers (`NDS_init`/`NDS_isExists`/`NDS_get`/`NDS_put`) — identical ABI to mock

Constants: `kBlockSize = 4MB`, `kAlignment = 4096`, `kBounceBufferSize = 2MB`.

### Step 2: Create `mooncake-store/src/nds/CMakeLists.txt`

```cmake
set(NDS_PRODUCTION_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/nds_production.cpp PARENT_SCOPE)
```

### Step 3: Add `USE_NDS` cmake option

In top-level `CMakeLists.txt`:
```cmake
option(USE_NDS "Enable production NDS (Network Direct Storage) on NVMe" OFF)
```

### Step 4: Modify `mooncake-store/src/CMakeLists.txt`

- After the `if(USE_3FS)` block: add `if(USE_NDS)` with `add_subdirectory(nds)` and `add_definitions(-DUSE_NDS)`
- Modify `target_link_libraries`: conditionally exclude `ndsclient` when `USE_NDS` is ON (symbols come from compiled-in `nds_production.cpp`)

### Step 5: Modify `mooncake-store/include/storage_backend.h`

Add three fields to `StorageBackend` private section (unconditional, after line ~477):
```cpp
bool use_nds_{false};
std::string nds_device_path_;
uint64_t nds_capacity_{0};
```

Extend `Create` factory signature to accept optional NDS params:
```cpp
static std::shared_ptr<StorageBackend> Create(
    const std::string& root_dir, const std::string& fsdir,
    bool enable_eviction = true,
    bool use_nds = false,
    const std::string& nds_device_path = "",
    uint64_t nds_capacity = 0);
```

Inside `Create`, add after the `std::make_shared` call:
```cpp
#ifdef USE_NDS
backend->use_nds_ = use_nds;
backend->nds_device_path_ = nds_device_path;
backend->nds_capacity_ = nds_capacity;
#endif
```

### Step 6: Modify `mooncake-store/src/storage_backend.cpp` — Init

At the top of `Init()` (after line 121), add:
```cpp
#ifdef USE_NDS
if (use_nds_) {
    int32_t result = NDS::init(
        const_cast<char*>(nds_device_path_.c_str()), nds_capacity_);
    if (result != 0) {
        LOG(ERROR) << "NDS::init failed with code " << result;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    initialized_.store(true, std::memory_order_release);
    return {};
}
#endif
```

### Step 7: Modify `IsEvictionEnabled()` 

Add at top of the method:
```cpp
#ifdef USE_NDS
if (use_nds_) return false;
#endif
```

### Step 8: Modify `StoreObject(slices, key)` (line 288)

Wrap the NDS block in `#ifdef USE_NDS` / `if (use_nds_)`. Keep the current NDS logic as-is. Replace the `#if 0` block (lines 319-367) with an `#else` branch so the old file-based path is the fallback:
```cpp
#ifdef USE_NDS
if (use_nds_) {
    // ... current NDS::put code (lines 291-317) ...
    return {};
}
#endif
// ... old file-based code (currently inside #if 0, lines 321-366) ...
```

Also remove the commented-out line 292 (`// ObjectKey key = ExtractKeyFromPath(path);`).

### Step 9: Modify `StoreObject(string, key)` (line 369) and `StoreObject(span, key)` (line 375)

Add `#ifdef USE_NDS` guard at the top of each. When NDS is active, convert to buffer and call `NDS::put` directly, skipping file I/O entirely. The string overload can call `NDS::put` directly without slice conversion.

### Step 10: Modify `LoadObject(slices, length)` (line 419) and `LoadObject(string, length)` (line 498)

Add `#ifdef USE_NDS` guard at the top of each:
- Extract key from path via `ExtractKeyFromPath(path)` (at [types.h:171](mooncake-store/include/types.h#L171))
- Convert to blockId via `objectKeyToUint64(key)` (at [types.h:151](mooncake-store/include/types.h#L151))
- Call `NDS::get(blockId, buffer, 0, length)`
- For slices overload: read into contiguous buffer, then scatter into pre-allocated slices
- For string overload: resize string and read directly into it

### Step 11: Modify `PutToLocalFile` in `client_service.cpp` (line 2104)

Change from calling `StoreObject(path, value, key)` (string overload) to calling `StoreObject(path, slices, key)` (slices overload). This is a one-line change:
```cpp
// Before:
auto store_result = backend->StoreObject(path, value, key);
// After:
auto store_result = backend->StoreObject(path, slices, key);
```

This eliminates code duplication — no `#ifdef` needed in `client_service.cpp`. When NDS is enabled, the slices overload routes to `NDS::put`; when disabled, it routes through the file-based fallback. The string concatenation (`value.append`) can be removed since the slices overload handles it internally.

### Step 12: Update `PrepareStorageBackend` in `client_service.cpp` (line 2088)

Pass NDS configuration from environment variables to `StorageBackend::Create`:
```cpp
bool use_nds = (getenv("MOONCAKE_USE_NDS") != nullptr);
std::string nds_device = getenv("MOONCAKE_NDS_DEVICE_PATH") ?: "";
uint64_t nds_cap = std::stoull(getenv("MOONCAKE_NDS_CAPACITY_GB") ?: "0") * 1024ULL * 1024 * 1024;
storage_backend_ = StorageBackend::Create(storage_root_dir, fsdir,
                                          enable_eviction, use_nds,
                                          nds_device, nds_cap);
```

### Step 13: Create `mooncake-store/tests/nds_production_test.cpp`

Unit tests using a temporary file as simulated block device:
- `PutGetRoundtrip` — basic aligned write/read
- `PutGetUnaligned` — unaligned data (offset=1, len=100)
- `IsExistsBasic` — check written/existing blocks
- `ReadBeforeWrite` — expect error

## Files Modified/Created

| File | Action |
|---|---|
| `mooncake-store/src/nds/nds_production.cpp` | **NEW** |
| `mooncake-store/src/nds/CMakeLists.txt` | **NEW** |
| `mooncake-store/src/CMakeLists.txt` | Modify |
| `CMakeLists.txt` (top-level) | Modify |
| `mooncake-store/include/storage_backend.h` | Modify |
| `mooncake-store/src/storage_backend.cpp` | Modify |
| `mooncake-store/src/client_service.cpp` | Modify (lines 2088-2165) |
| `mooncake-store/tests/nds_production_test.cpp` | **NEW** |

## Verification

1. **Build**: `cmake -DUSE_NDS=ON .. && make mooncake_store` — verify compilation succeeds
2. **Build without NDS**: `cmake -DUSE_NDS=OFF .. && make mooncake_store` — verify mock `libndsclient` is linked, old behavior preserved
3. **Unit tests**: Run `nds_production_test` on a filesystem that supports O_DIRECT (tmpfs/XFS/ext4)
4. **Integration test**: Run existing `client_gds_integration_test` with `USE_NDS=ON`
5. **Mock tests**: Run `make test` in `include/nds/` to verify mock still works standalone