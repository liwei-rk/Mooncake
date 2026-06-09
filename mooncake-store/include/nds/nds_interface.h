#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>

namespace NDS {
    int32_t init(void *memAddr, uint64_t length, const char *path_nds_config = nullptr);

    int32_t isExists(const std::vector<std::pair<uint64_t, uint64_t>>& blockIds);

    int32_t get(uint64_t keyHigh, uint64_t keyLow, uint8_t *blockAddr, size_t offset, size_t length, uint32_t nsid);

    int32_t batchGet(const std::vector<std::pair<uint64_t, uint64_t>>& blockIds,
                                std::vector<uint8_t *> blockAddrs,
                                std::vector<size_t> offsets, std::vector<size_t> lengths,
                                std::vector<uint32_t> nsids);

    int32_t put(uint64_t keyHigh, uint64_t keyLow, uint8_t *blockAddr, size_t offset, size_t length, uint32_t nsid);

    int32_t batchPut(const std::vector<std::pair<uint64_t, uint64_t>>& blockIds,
                                std::vector<uint8_t *> blockAddrs,
                                std::vector<size_t> offsets, std::vector<size_t> lengths,
                                std::vector<uint32_t> nsids);
}

extern "C" {
    int32_t c_init(void *memAddr, uint64_t length, const char *path_nds_config);

    int32_t c_isExists(const uint64_t *keyHighs, const uint64_t *keyLows, size_t count);

    int32_t c_get(uint64_t keyHigh, uint64_t keyLow, uint8_t *blockAddr, size_t offset, size_t length, uint32_t nsid);

    int32_t c_batchGet(const uint64_t *keyHighs, const uint64_t *keyLows,
                                  uint8_t **blockAddrs,
                                  const size_t *offsets, const size_t *lengths,
                                  const uint32_t *nsids, size_t count);

    int32_t c_put(uint64_t keyHigh, uint64_t keyLow, uint8_t *blockAddr, size_t offset, size_t length, uint32_t nsid);

    int32_t c_batchPut(const uint64_t *keyHighs, const uint64_t *keyLows,
                                  uint8_t **blockAddrs,
                                  const size_t *offsets, const size_t *lengths,
                                  const uint32_t *nsids, size_t count);
}