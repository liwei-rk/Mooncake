#pragma once

#include <glog/logging.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "types.h"
#include <ylt/util/tl/expected.hpp>

namespace mooncake {

class KVStorageBackend {
   public:
    KVStorageBackend() = default;

    ~KVStorageBackend();

    // WHY accumulate-instead-of-init: NDS nds_init() is strictly one-shot
    // (each call rebuilds RDMA connections and spawns duplicate poll
    // threads), and every buffer that later feeds batch_put_from /
    // batch_get_into must already be an MR at that single call. Therefore
    // RegisterLocalMemory only appends regions here (setup's local buffer
    // plus every register_buffer caller, e.g. sglang HiCache host pools or
    // the vLLM staging buffer); the actual NDS init happens lazily at the
    // first NDS data operation via EnsureInitialized().
    tl::expected<void, ErrorCode> Init(void* nds_mem_addr = nullptr,
                                       uint64_t nds_mem_size = 0);

    // One-shot multi-MR NDS init with all accumulated regions. Called by
    // the batch put / get paths right before the first NDS transfer.
    // Concurrent-safe; no-op after first success.
    tl::expected<void, ErrorCode> EnsureInitialized();

    // Drop a region from the pending list (pre-init unregister).
    // After NDS init succeeded regions cannot be removed (nds_init is
    // one-shot); late unregisters are tolerated with a warning because a
    // stale MR entry is only consulted for our own batch operations.
    void RemoveRegion(void* addr);

    void CleanupNDS();

    bool isInitialized() const {
        return initialized_.load(std::memory_order_acquire);
    }

    tl::expected<std::vector<std::string>, ErrorCode> StoreObjects(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<Slice>>& batched_slices);

    tl::expected<void, ErrorCode> LoadObjects(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<Slice>>& batched_slices);

    void Remove(const std::string& key);
    void RemoveByRegex(const std::string& key);
    void RemoveAll();

    void setNsid(uint32_t nsid) { nsid_ = nsid; }
    uint32_t nsid() const { return nsid_; }

    private:
    bool owns_nds_memory_{false};
    void* nds_mem_addr_ = nullptr;
    uint64_t nds_mem_size_ = 0;
    uint32_t nsid_{0};
    std::atomic<bool> initialized_{false};
    std::mutex init_mu_;
    std::mutex regions_mu_;
    std::vector<std::pair<void*, uint64_t>> regions_;
};

}  // namespace mooncake