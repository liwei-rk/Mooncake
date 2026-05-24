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

    tl::expected<std::vector<std::string>, ErrorCode> StoreObjects(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<Slice>>& batched_slices);

    tl::expected<void, ErrorCode> LoadObjects(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<Slice>>& batched_slices);

    void Remove(const std::string& key);
    void RemoveByRegex(const std::string& key);
    void RemoveAll();

    private:
    bool owns_nds_memory_{false};
    void* nds_mem_addr_ = nullptr;
    uint64_t nds_mem_size_ = 0;
    std::atomic<bool> initialized_{false};
};

}  // namespace mooncake