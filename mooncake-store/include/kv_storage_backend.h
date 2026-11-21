#pragma once

#include <glog/logging.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "types.h"
#include <ylt/util/tl/expected.hpp>

namespace mooncake {

class KVStorageBackend {
   public:
    KVStorageBackend() = default;

    ~KVStorageBackend();

    tl::expected<void, ErrorCode> Init(void* nds_mem_addr = nullptr,
                                       uint64_t nds_mem_size = 0);

    /**
     * @brief Initialize NDS KV storage with multiple external memory regions
     * @param mem_addrs Addresses of the memory regions
     * @param mem_sizes Sizes of the memory regions in bytes
     * @return ErrorCode indicating success/failure
     * @note Falls back to single-region Init() with the first region when the
     * underlying NDS library does not export initMulti
     */
    tl::expected<void, ErrorCode> InitMulti(
        const std::vector<void*>& mem_addrs,
        const std::vector<uint64_t>& mem_sizes);

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
};

}  // namespace mooncake