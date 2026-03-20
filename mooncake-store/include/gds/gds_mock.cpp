#include "gds_mock.h"
#include <cstring>
#include <algorithm>

namespace GDS {

GDSMock& GDSMock::instance() {
    static GDSMock instance;
    return instance;
}

int32_t GDSMock::init(void* addr, uint64_t len) {
    std::lock_guard<std::mutex> lock(mutex_);
    initialized_ = true;
    return 0;  // Success
}

int32_t GDSMock::isExists(std::vector<uint64_t> blockIds) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!initialized_) {
        return -1;  // Not initialized
    }
    
    int32_t count = 0;
    for (uint64_t blockId : blockIds) {
        if (data_.find(blockId) != data_.end()) {
            ++count;
        }
    }
    return count;
}

int32_t GDSMock::get(uint64_t blockId, uint8_t* blockAddr, size_t offset, size_t len) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!initialized_) {
        return -1;  // Not initialized
    }
    
    auto it = data_.find(blockId);
    if (it == data_.end()) {
        return -2;  // Block not found
    }
    
    const std::vector<uint8_t>& data = it->second;
    
    // Check bounds
    if (offset + len > data.size()) {
        return -3;  // Out of bounds
    }
    
    // Copy data
    std::memcpy(blockAddr, data.data() + offset, len);
    return 0;  // Success
}

int32_t GDSMock::put(uint64_t blockId, uint8_t* blockAddr, size_t offset, size_t len) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!initialized_) {
        return -1;  // Not initialized
    }
    
    // Create or resize the data buffer
    std::vector<uint8_t>& data = data_[blockId];
    
    // Resize if necessary
    size_t required_size = offset + len;
    if (data.size() < required_size) {
        data.resize(required_size);
    }
    
    // Copy data
    std::memcpy(data.data() + offset, blockAddr, len);
    return 0;  // Success
}

void GDSMock::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    data_.clear();
}

size_t GDSMock::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.size();
}

bool GDSMock::hasBlock(uint64_t blockId) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.find(blockId) != data_.end();
}

// C interface functions for linking
extern "C" {

int32_t GDS_init(void* addr, uint64_t len) {
    return GDSMock::instance().init(addr, len);
}

int32_t GDS_isExists(uint64_t* blockIds, int32_t count) {
    std::vector<uint64_t> ids(blockIds, blockIds + count);
    return GDSMock::instance().isExists(ids);
}

int32_t GDS_get(uint64_t blockId, uint8_t* blockAddr, size_t offset, size_t len) {
    return GDSMock::instance().get(blockId, blockAddr, offset, len);
}

int32_t GDS_put(uint64_t blockId, uint8_t* blockAddr, size_t offset, size_t len) {
    return GDSMock::instance().put(blockId, blockAddr, offset, len);
}

} // extern "C"

} // namespace GDS
