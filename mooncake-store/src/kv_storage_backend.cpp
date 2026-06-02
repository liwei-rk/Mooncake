#include "kv_storage_backend.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <dlfcn.h>
#include <unordered_map>

namespace mooncake {

namespace {

typedef int32_t (*NDS_init_fn)(void*, uint64_t);
typedef int32_t (*NDS_isExists_fn)(const uint64_t*, size_t);
typedef int32_t (*NDS_get_fn)(uint64_t, uint8_t*, size_t, size_t, uint32_t);
typedef int32_t (*NDS_put_fn)(uint64_t, uint8_t*, size_t, size_t, uint32_t);
typedef int32_t (*NDS_batchGet_fn)(const uint64_t*, uint8_t**, const size_t*,
                                   const size_t*, const uint32_t*, size_t);
typedef int32_t (*NDS_batchPut_fn)(const uint64_t*, uint8_t**, const size_t*,
                                   const size_t*, const uint32_t*, size_t);

struct NDSLoader {
    void* handle = nullptr;
    NDS_init_fn init = nullptr;
    NDS_isExists_fn isExists = nullptr;
    NDS_get_fn get = nullptr;
    NDS_put_fn put = nullptr;
    NDS_batchGet_fn batchGet = nullptr;
    NDS_batchPut_fn batchPut = nullptr;
    bool nds_initialized = false;
    void* nds_mem_addr = nullptr;
    uint64_t nds_mem_size = 0;

    static NDSLoader& Instance() {
        static NDSLoader instance;
        return instance;
    }

    bool Load() {
        if (handle) return true;

        const char* env_path = std::getenv("NDS_LIBRARY_PATH");
        std::string lib_path = env_path ? env_path : "libndskv.so";

        handle = dlopen(lib_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (!handle) {
            // LOG(ERROR) << "Failed to load " << lib_path << ": " << dlerror();
            return false;
        }

        init = (NDS_init_fn)dlsym(handle, "init");
        isExists = (NDS_isExists_fn)dlsym(handle, "isExists");
        get = (NDS_get_fn)dlsym(handle, "get");
        put = (NDS_put_fn)dlsym(handle, "put");
        batchGet = (NDS_batchGet_fn)dlsym(handle, "batchGet");
        batchPut = (NDS_batchPut_fn)dlsym(handle, "batchPut");

        if (!init || !get || !put || !batchGet || !batchPut) {
            LOG(ERROR) << "Failed to load NDS functions";
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
    if (initialized_.load(std::memory_order_acquire)) {
        LOG(WARNING) << "KVStorageBackend is already initialized. Skipping.";
        return {};
    }

    bool nds_loaded = NDSLoader::Instance().Load();
    if (!nds_loaded) {
        LOG(ERROR) << "KVStorageBackend requires NDS library but failed to load";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    auto& loader = NDSLoader::Instance();

    if (!loader.nds_initialized) {
        if (nds_mem_addr && nds_mem_size > 0) {
            nds_mem_addr_ = nds_mem_addr;
            nds_mem_size_ = nds_mem_size;
            int32_t result = loader.init(nds_mem_addr_, nds_mem_size_);
            if (result != 0) {
                LOG(ERROR) << "Failed to initialize NDS KV storage with external memory: " << result;
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            LOG(INFO) << "NDS KV storage initialized with external memory, size: "
                      << nds_mem_size_ << " bytes";
            owns_nds_memory_ = false;
            loader.nds_initialized = true;
            loader.nds_mem_addr = nds_mem_addr_;
            loader.nds_mem_size = nds_mem_size_;
        } else {
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

            int32_t result = loader.init(nds_mem_addr_, nds_mem_size_);
            if (result != 0) {
                LOG(ERROR) << "Failed to initialize NDS KV storage: " << result;
                free(nds_mem_addr_);
                nds_mem_addr_ = nullptr;
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }

            LOG(INFO) << "NDS KV storage initialized, size: " << nds_mem_size_
                      << " bytes";
            owns_nds_memory_ = true;
            loader.nds_initialized = true;
            loader.nds_mem_addr = nds_mem_addr_;
            loader.nds_mem_size = nds_mem_size_;
        }
    } else {
        nds_mem_addr_ = loader.nds_mem_addr;
        nds_mem_size_ = loader.nds_mem_size;
        LOG(INFO) << "NDS KV storage already initialized, reusing existing instance";
    }

    initialized_.store(true, std::memory_order_release);
    return {};
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
    auto t0 = std::chrono::steady_clock::now();
    std::vector<uint64_t> blockIds;
    std::vector<uint8_t*> blockAddrs;
    std::vector<size_t> nds_offsets;
    std::vector<size_t> nds_lengths;

    // Each key's slices come from a single contiguous client buffer (split by
    // kMaxSliceSize in batch_put_from_internal). NDS requires contiguous data
    // for zero-copy writes: if slices are contiguous in memory, we merge them
    // into a single NDS entry covering the entire object; if not contiguous,
    // this violates the expected invariant and we fail immediately.
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

    auto t_flatten_end = std::chrono::steady_clock::now();
    LOG(INFO) << "[BatchPut] KVStoreObjects flatten slices: "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     t_flatten_end - t0).count() << " us";

    auto& loader = NDSLoader::Instance();
    std::vector<uint32_t> nsids(blockIds.size(), nsid_);
    int32_t result = loader.batchPut(blockIds.data(), blockAddrs.data(),
                                     nds_offsets.data(), nds_lengths.data(),
                                     nsids.data(), blockIds.size());
    auto t_batchput_end = std::chrono::steady_clock::now();
    LOG(INFO) << "[BatchPut] KVStoreObjects NDS batchPut call: "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     t_batchput_end - t_flatten_end).count() << " us";
    if (result != 0) {
        // LOG(ERROR) << "NDS batchPut failed: " << result
        //            << " for " << blockIds.size() << " slices";
        return tl::unexpected(ErrorCode::WRITE_FAIL);
    }

    return std::vector<std::string>{};
}

tl::expected<void, ErrorCode> KVStorageBackend::LoadObjects(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<Slice>>& batched_slices) {
    std::vector<uint64_t> blockIds;
    std::vector<uint8_t*> blockAddrs;
    std::vector<size_t> nds_offsets;
    std::vector<size_t> nds_lengths;

    // Each key's slices come from a single contiguous client buffer (split by
    // kMaxSliceSize in batch_get_from_internal). NDS requires contiguous data
    // for zero-copy reads: if slices are contiguous in memory, we merge them
    // into a single NDS entry covering the entire object; if not contiguous,
    // this violates the expected invariant and we fail immediately.
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
    int32_t result = loader.batchGet(blockIds.data(), blockAddrs.data(),
                                     nds_offsets.data(), nds_lengths.data(),
                                     nsids.data(), blockIds.size());
    if (result != 0) {
        // LOG(ERROR) << "NDS batchGet failed: " << result
        //            << " for " << blockIds.size() << " slices";
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