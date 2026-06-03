#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>

namespace NDS {
    int32_t init(void *memAddr, uint64_t length);

    int32_t isExists(std::vector<uint64_t> blockIds);

    int32_t get(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t length, uint32_t nsid);

    int32_t batchGet(std::vector<uint64_t> blockIds, std::vector<uint8_t *> blockAddrs,
         std::vector<size_t> offsets, std::vector<size_t> lengths, std::vector<uint32_t> nsids);

    int32_t put(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t length, uint32_t nsid);

    int32_t batchPut(std::vector<uint64_t> blockIds, std::vector<uint8_t *> blockAddrs,
         std::vector<size_t> offsets, std::vector<size_t> lengths, std::vector<uint32_t> nsids);
}

extern "C" {
    int32_t init(void *memAddr, uint64_t length);

    int32_t isExists(const uint64_t *blockIds, size_t count);

    int32_t get(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t length, uint32_t nsid);

    int32_t batchGet(const uint64_t *blockIds, uint8_t **blockAddrs,
         const size_t *offsets, const size_t *lengths, const uint32_t *nsids, size_t count);

    int32_t put(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t length, uint32_t nsid);

    int32_t batchPut(const uint64_t *blockIds, uint8_t **blockAddrs,
         const size_t *offsets, const size_t *lengths, const uint32_t *nsids, size_t count);
}
