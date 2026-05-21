#include "nds_interface.h"

extern "C" int32_t init(void *memAddr, uint64_t length) {
    return NDS::init(memAddr, length);
}

extern "C" int32_t isExists(uint64_t *blockIds, int32_t count) {
    std::vector<uint64_t> ids(blockIds, blockIds + count);
    return NDS::isExists(std::move(ids));
}

extern "C" int32_t get(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t length) {
    return NDS::get(blockId, blockAddr, offset, length);
}

extern "C" int32_t put(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t length) {
    return NDS::put(blockId, blockAddr, offset, length);
}

extern "C" int32_t batchGet(uint64_t *blockIds, uint8_t **blockAddrs, size_t *offsets,
                            size_t *lengths, int32_t count) {
    std::vector<uint64_t> ids(blockIds, blockIds + count);
    std::vector<uint8_t *> addrs(blockAddrs, blockAddrs + count);
    std::vector<size_t> offs(offsets, offsets + count);
    std::vector<size_t> lens(lengths, lengths + count);
    return NDS::batchGet(std::move(ids), std::move(addrs),
                         std::move(offs), std::move(lens));
}

extern "C" int32_t batchPut(uint64_t *blockIds, uint8_t **blockAddrs, size_t *offsets,
                            size_t *lengths, int32_t count) {
    std::vector<uint64_t> ids(blockIds, blockIds + count);
    std::vector<uint8_t *> addrs(blockAddrs, blockAddrs + count);
    std::vector<size_t> offs(offsets, offsets + count);
    std::vector<size_t> lens(lengths, lengths + count);
    return NDS::batchPut(std::move(ids), std::move(addrs),
                         std::move(offs), std::move(lens));
}