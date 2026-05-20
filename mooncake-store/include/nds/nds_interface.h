#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>

namespace NDS {
    /**
     * @brief Initialize NDS KV storage
     * @param addr Memory address for initialization
     * @param len Length of memory region
     * @return 0 on success, error code on failure
     */
    int32_t init(void *memAddr, uint64_t length);

    /**
     * @brief Check if block IDs exist in NDS KV storage
     * @param blockIds Vector of block IDs to check
     * @return Count of existing block IDs in the input vector
     */
    int32_t isExists(std::vector<uint64_t> blockIds);

    /**
     * @brief Get data from NDS KV storage
     * @param blockId Block ID to retrieve
     * @param blockAddr Buffer to store retrieved data
     * @param offset Offset within the block
     * @param len Length of data to retrieve
     * @return 0 on success, error code on failure
     */
    int32_t get(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t length);

    /**
     * @brief Put data into NDS KV storage
     * @param blockId Block ID to store
     * @param blockAddr Buffer containing data to store
     * @param offset Offset within the block
     * @param len Length of data to store
     * @return 0 on success, error code on failure
     */
    int32_t put(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t length);

    int32_t batchGet(std::vector<uint64_t> blockIds, std::vector<uint8_t *> blockAddrs, std::vector<size_t> offsets, std::vector<size_t> lengths);

    int32_t batchPut(std::vector<uint64_t> blockIds, std::vector<uint8_t *> blockAddrs,
         std::vector<size_t> offsets, std::vector<size_t> lengths);
}
