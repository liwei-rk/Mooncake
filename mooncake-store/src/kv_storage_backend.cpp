#include "kv_storage_backend.h"

#include <algorithm>
#include <cstring>
#include <dlfcn.h>
#include <unordered_map>

namespace mooncake {

namespace {

typedef int32_t (*NDS_init_fn)(void*, uint64_t, const char*);
typedef int32_t (*NDS_initMulti_fn)(const void* const*, const uint64_t*, size_t, const char*);
typedef int32_t (*NDS_isExists_fn)(const uint64_t*, size_t, uint32_t);
typedef int32_t (*NDS_get_fn)(uint64_t, uint8_t*, size_t, size_t, uint32_t);
typedef int32_t (*NDS_put_fn)(uint64_t, uint8_t*, size_t, size_t, uint32_t);
typedef int32_t (*NDS_batchGet_fn)(const uint64_t*, uint8_t**,
                                   const size_t*, const size_t*, const uint32_t*, size_t);
typedef int32_t (*NDS_batchPut_fn)(const uint64_t*, uint8_t**,
                                   const size_t*, const size_t*, const uint32_t*, size_t);

struct NDSLoader {
    void* handle = nullptr;
    NDS_init_fn init = nullptr;
    NDS_initMulti_fn initMulti = nullptr;
    NDS_isExists_fn isExists = nullptr;
    NDS_get_fn get = nullptr;
    NDS_put_fn put = nullptr;
    NDS_batchGet_fn batchGet = nullptr;
    NDS_batchPut_fn batchPut = nullptr;
    bool nds_initialized = false;
    void* nds_mem_addr = nullptr;
    uint64_t nds_mem_size = 0;
    std::string nds_config_path;

    static NDSLoader& Instance() {
        static NDSLoader instance;
        return instance;
    }

    bool Load() {
        if (handle) return true;

        const char* env_path = std::getenv("NDS_LIBRARY_PATH");
        std::string lib_path = env_path ? env_path : "libndskv.so";

        const char* env_config = std::getenv("MC_NDS_CONFIG");
        nds_config_path = env_config ? env_config : "nds_config.conf";

        handle = dlopen(lib_path.c_str(), RTLD_NOW | RTLD_GLOBAL);
        if (!handle) {
            LOG(ERROR) << "Failed to load " << lib_path << ": " << dlerror();
            return false;
        }

        dlerror();
        init = (NDS_init_fn)dlsym(handle, "c_init");
        const char* dlsym_error = dlerror();
        if (dlsym_error) { LOG(ERROR) << "dlsym 'c_init' failed: " << dlsym_error; }

        dlerror();
        initMulti = (NDS_initMulti_fn)dlsym(handle, "c_init_multi");
        dlsym_error = dlerror();
        // Optional: old libndskv.so builds only export single-region c_init.
        // Multi-region accumulation then degrades to first-region-only init,
        // which breaks late register_buffer() callers (loudly, via warning
        // in EnsureInitialized).
        if (dlsym_error) { LOG(WARNING) << "dlsym 'c_init_multi' unavailable: " << dlsym_error; }

        dlerror();
        isExists = (NDS_isExists_fn)dlsym(handle, "c_isExists");
        dlsym_error = dlerror();
        if (dlsym_error) { LOG(ERROR) << "dlsym 'c_isExists' failed: " << dlsym_error; }

        dlerror();
        get = (NDS_get_fn)dlsym(handle, "c_get");
        dlsym_error = dlerror();
        if (dlsym_error) { LOG(ERROR) << "dlsym 'c_get' failed: " << dlsym_error; }

        dlerror();
        put = (NDS_put_fn)dlsym(handle, "c_put");
        dlsym_error = dlerror();
        if (dlsym_error) { LOG(ERROR) << "dlsym 'c_put' failed: " << dlsym_error; }

        dlerror();
        batchGet = (NDS_batchGet_fn)dlsym(handle, "c_batchGet");
        dlsym_error = dlerror();
        if (dlsym_error) { LOG(ERROR) << "dlsym 'c_batchGet' failed: " << dlsym_error; }

        dlerror();
        batchPut = (NDS_batchPut_fn)dlsym(handle, "c_batchPut");
        dlsym_error = dlerror();
        if (dlsym_error) { LOG(ERROR) << "dlsym 'c_batchPut' failed: " << dlsym_error; }

        if (!init || !get || !put || !batchGet || !batchPut) {
            LOG(ERROR) << "Failed to load required NDS functions"
                       << " (init=" << (init ? "ok" : "MISSING")
                       << " get=" << (get ? "ok" : "MISSING")
                       << " put=" << (put ? "ok" : "MISSING")
                       << " batchGet=" << (batchGet ? "ok" : "MISSING")
                       << " batchPut=" << (batchPut ? "ok" : "MISSING")
                       << ")";
            dlclose(handle);
            handle = nullptr;
            return false;
        }

        LOG(INFO) << "Successfully loaded libndskv.so";
        return true;
    }

    void Unload() {
        if (handle) {
            dlclose(handle);
            handle = nullptr;
        }
    }
};

}  // namespace

KVStorageBackend::~KVStorageBackend() {
    CleanupNDS();
}

tl::expected<void, ErrorCode> KVStorageBackend::Init(void* nds_mem_addr,
                                                     uint64_t nds_mem_size) {
    // WHY accumulate-only: nds_init() is one-shot per process (see header).
    // Every RegisterLocalMemory call (setup's local buffer, sglang host
    // pools, vLLM staging) appends here; the single init happens in
    // EnsureInitialized() at the first NDS data operation, when all regions
    // are known. Burning the one-shot on the first region would silently
    // exclude every later register_buffer() region from the MR set and
    // batch_put_from would degrade to a memory replica (no disk write).
    if (!nds_mem_addr || nds_mem_size == 0) {
        return {};
    }
    if (initialized_.load(std::memory_order_acquire)) {
        LOG(WARNING) << "NDS already initialized; region " << nds_mem_addr
                     << " (" << nds_mem_size
                     << " bytes) registered too late for the MR set. "
                     << "Zero-copy NDS access to it will fail.";
        // Still track it for diagnostics.
        std::lock_guard<std::mutex> lk(regions_mu_);
        for (auto& r : regions_) {
            if (r.first == nds_mem_addr) { r.second = nds_mem_size; return {}; }
        }
        regions_.emplace_back(nds_mem_addr, nds_mem_size);
        return {};
    }
    std::lock_guard<std::mutex> lk(regions_mu_);
    for (auto& r : regions_) {
        if (r.first == nds_mem_addr) { r.second = nds_mem_size; return {}; }
    }
    regions_.emplace_back(nds_mem_addr, nds_mem_size);
    return {};
}

tl::expected<void, ErrorCode> KVStorageBackend::EnsureInitialized() {
    if (initialized_.load(std::memory_order_acquire)) {
        return {};
    }

    // Serialize first-init: batch operations run on worker threads, so the
    // first NDS transfer can race from several threads at once. nds_init()
    // must execute exactly once (poll threads, QPs, cid pool are global).
    std::lock_guard<std::mutex> init_lk(init_mu_);
    if (initialized_.load(std::memory_order_acquire)) {
        return {};
    }

    bool nds_loaded = NDSLoader::Instance().Load();
    if (!nds_loaded) {
        LOG(ERROR) << "KVStorageBackend requires NDS library but failed to load";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    auto& loader = NDSLoader::Instance();

    if (!loader.nds_initialized) {
        std::vector<std::pair<void*, uint64_t>> snapshot;
        {
            std::lock_guard<std::mutex> lk(regions_mu_);
            snapshot = regions_;
        }

        int32_t result = 0;
        if (snapshot.empty()) {
            // No region registered yet: self-allocate 1GB (legacy fallback,
            // kept for callers that never went through RegisterLocalMemory).
            nds_mem_size_ = 1024 * 1024 * 1024;

            constexpr size_t kNDSAlignment = 4096;
            size_t aligned_size =
                ((nds_mem_size_ + kNDSAlignment - 1) / kNDSAlignment) *
                kNDSAlignment;

            nds_mem_addr_ = std::aligned_alloc(kNDSAlignment, aligned_size);
            if (!nds_mem_addr_) {
                LOG(ERROR) << "Failed to allocate 4096-aligned memory for NDS: "
                           << aligned_size << " bytes";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }

            result = loader.init(nds_mem_addr_, nds_mem_size_, loader.nds_config_path.c_str());
            if (result != 0) {
                LOG(ERROR) << "Failed to initialize NDS KV storage: " << result;
                free(nds_mem_addr_);
                nds_mem_addr_ = nullptr;
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }

            LOG(INFO) << "NDS KV storage initialized, size: " << nds_mem_size_
                      << " bytes";
            owns_nds_memory_ = true;
            loader.nds_mem_addr = nds_mem_addr_;
            loader.nds_mem_size = nds_mem_size_;
        } else if (loader.initMulti) {
            // WHY c_init_multi: one nds_init() call carrying every
            // accumulated region (local buffer + all register_buffer
            // callers), so batch_put_from / batch_get_into on any of them
            // resolves inside the MR set.
            std::vector<const void*> addrs;
            std::vector<uint64_t> lens;
            addrs.reserve(snapshot.size());
            lens.reserve(snapshot.size());
            for (const auto& r : snapshot) {
                addrs.push_back(r.first);
                lens.push_back(r.second);
            }
            result = loader.initMulti(addrs.data(), lens.data(), addrs.size(),
                                      loader.nds_config_path.c_str());
            if (result != 0) {
                LOG(ERROR) << "Failed to initialize NDS KV storage with "
                           << addrs.size() << " MR(s): " << result;
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            LOG(INFO) << "NDS KV storage initialized with external memory, "
                      << addrs.size() << " MR(s), total "
                      << [&lens] {
                             uint64_t sum = 0;
                             for (auto l : lens) sum += l;
                             return sum;
                         }()
                      << " bytes";
            owns_nds_memory_ = false;
            loader.nds_mem_addr = snapshot.front().first;
            loader.nds_mem_size = snapshot.front().second;
        } else {
            LOG(WARNING) << "c_init_multi unavailable; falling back to "
                           "single-region c_init (later regions will fail MR lookup)";
            result = loader.init(snapshot.front().first, snapshot.front().second,
                                loader.nds_config_path.c_str());
            if (result != 0) {
                LOG(ERROR) << "Failed to initialize NDS KV storage with external memory: " << result;
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            LOG(INFO) << "NDS KV storage initialized with external memory (single MR), size: "
                      << snapshot.front().second << " bytes";
            owns_nds_memory_ = false;
            loader.nds_mem_addr = snapshot.front().first;
            loader.nds_mem_size = snapshot.front().second;
        }
        loader.nds_initialized = true;
    } else {
        nds_mem_addr_ = loader.nds_mem_addr;
        nds_mem_size_ = loader.nds_mem_size;
        LOG(INFO) << "NDS KV storage already initialized, reusing existing instance";
    }

    initialized_.store(true, std::memory_order_release);
    return {};
}

void KVStorageBackend::RemoveRegion(void* addr) {
    if (initialized_.load(std::memory_order_acquire)) {
        // WHY no cleanup here: nds_init is one-shot; tearing down the whole
        // MR set because one buffer was unregistered would break every
        // remaining region. A stale MR entry is only consulted by our own
        // batch operations, so keeping it is safe.
        LOG(WARNING) << "NDS already initialized; region " << addr
                     << " stays in the MR set until teardown";
        return;
    }
    std::lock_guard<std::mutex> lk(regions_mu_);
    regions_.erase(std::remove_if(regions_.begin(), regions_.end(),
                                  [addr](const std::pair<void*, uint64_t>& r) {
                                      return r.first == addr;
                                  }),
                   regions_.end());
}

void KVStorageBackend::CleanupNDS() {
    if (nds_mem_addr_ && owns_nds_memory_) {
        LOG(INFO) << "Cleaning up KV storage backend memory";
        free(nds_mem_addr_);
    }
    nds_mem_addr_ = nullptr;
    nds_mem_size_ = 0;
    owns_nds_memory_ = false;
    auto& loader = NDSLoader::Instance();
    loader.nds_initialized = false;
    loader.nds_mem_addr = nullptr;
    loader.nds_mem_size = 0;
    initialized_.store(false, std::memory_order_release);
}

tl::expected<std::vector<std::string>, ErrorCode> KVStorageBackend::StoreObjects(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<Slice>>& batched_slices) {
    if (!initialized_.load(std::memory_order_acquire)) {
        // All callers must go through EnsureInitialized(); this guard turns a
        // would-be null-function-pointer crash into a loud error.
        LOG(ERROR) << "NDS not initialized; call EnsureInitialized() first";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::vector<uint64_t> blockIds;
    std::vector<uint8_t*> blockAddrs;
    std::vector<size_t> nds_offsets;
    std::vector<size_t> nds_lengths;

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& slices = batched_slices[i];
        uint64_t blockId = objectKeyToUint64(keys[i]);
        size_t total_slice_size = 0;
        for (const auto& sl : slices) total_slice_size += sl.size;

        bool contiguous = true;
        for (size_t s = 1; s < slices.size(); ++s) {
            if (reinterpret_cast<uint8_t*>(slices[s - 1].ptr) + slices[s - 1].size !=
                reinterpret_cast<uint8_t*>(slices[s].ptr)) {
                contiguous = false;
                break;
            }
        }

        if (!contiguous) {
            LOG(ERROR) << "NDS StoreObjects: key=" << keys[i]
                       << " slices are not contiguous in memory, "
                       << "cannot perform zero-copy NDS write.";
            for (size_t s = 0; s < slices.size(); ++s) {
                LOG(ERROR) << "  slice[" << s << "]:"
                           << " ptr=" << slices[s].ptr
                           << " size=" << slices[s].size;
            }
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        blockIds.push_back(blockId);
        blockAddrs.push_back(reinterpret_cast<uint8_t*>(slices[0].ptr));
        nds_offsets.push_back(0);
        nds_lengths.push_back(total_slice_size);
    }

    auto& loader = NDSLoader::Instance();
    std::vector<uint32_t> nsids(blockIds.size(), nsid_);
    int32_t result = loader.batchPut(blockIds.data(),
                                     blockAddrs.data(), nds_offsets.data(),
                                     nds_lengths.data(), nsids.data(),
                                     blockIds.size());
    if (result != 0) {
        return tl::unexpected(ErrorCode::WRITE_FAIL);
    }

    return std::vector<std::string>{};
}

tl::expected<void, ErrorCode> KVStorageBackend::LoadObjects(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<Slice>>& batched_slices) {
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "NDS not initialized; call EnsureInitialized() first";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::vector<uint64_t> blockIds;
    std::vector<uint8_t*> blockAddrs;
    std::vector<size_t> nds_offsets;
    std::vector<size_t> nds_lengths;

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& slices = batched_slices[i];
        uint64_t blockId = objectKeyToUint64(keys[i]);
        size_t total_slice_size = 0;
        for (const auto& sl : slices) total_slice_size += sl.size;

        bool contiguous = true;
        for (size_t s = 1; s < slices.size(); ++s) {
            if (reinterpret_cast<uint8_t*>(slices[s - 1].ptr) + slices[s - 1].size !=
                reinterpret_cast<uint8_t*>(slices[s].ptr)) {
                contiguous = false;
                break;
            }
        }

        if (!contiguous) {
            LOG(ERROR) << "NDS LoadObjects: key=" << keys[i]
                       << " slices are not contiguous in memory, "
                       << "cannot perform zero-copy NDS read.";
            for (size_t s = 0; s < slices.size(); ++s) {
                LOG(ERROR) << "  slice[" << s << "]:"
                           << " ptr=" << slices[s].ptr
                           << " size=" << slices[s].size;
            }
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        blockIds.push_back(blockId);
        blockAddrs.push_back(reinterpret_cast<uint8_t*>(slices[0].ptr));
        nds_offsets.push_back(0);
        nds_lengths.push_back(total_slice_size);
    }

    auto& loader = NDSLoader::Instance();
    std::vector<uint32_t> nsids(blockIds.size(), nsid_);
    int32_t result = loader.batchGet(blockIds.data(),
                                     blockAddrs.data(), nds_offsets.data(),
                                     nds_lengths.data(), nsids.data(),
                                     blockIds.size());
    if (result != 0) {
        return tl::unexpected(ErrorCode::FILE_READ_FAIL);
    }

    return {};
}

void KVStorageBackend::Remove(const std::string& key) {
    // VLOG(0) << "[KVRemove] NDS KV store does not support deletion for key: " << key;
}

void KVStorageBackend::RemoveByRegex(const std::string& key) {
    // VLOG(0) << "[KVRemoveByRegex] NDS KV store does not support regex deletion: " << key;
}

void KVStorageBackend::RemoveAll() {
    // VLOG(0) << "[KVRemoveAll] NDS KV store does not support RemoveAll";
}

}  // namespace mooncake