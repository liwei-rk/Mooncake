#pragma once

#include "gds_interface.h"
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace GDS {

// Mock data storage for testing
class GDSMock {
public:
    static GDSMock& instance();
    
    // Mock implementations
    int32_t init(void* addr, uint64_t len);
    int32_t isExists(std::vector<uint64_t> blockIds);
    int32_t get(uint64_t blockId, uint8_t* blockAddr, size_t offset, size_t len);
    int32_t put(uint64_t blockId, uint8_t* blockAddr, size_t offset, size_t len);
    
    // Helper methods for testing
    void clear();
    size_t size() const;
    bool hasBlock(uint64_t blockId) const;
    
private:
    GDSMock() = default;
    ~GDSMock() = default;
    GDSMock(const GDSMock&) = delete;
    GDSMock& operator=(const GDSMock&) = delete;
    
    std::map<uint64_t, std::vector<uint8_t>> data_;
    mutable std::mutex mutex_;
    bool initialized_ = false;
};

} // namespace GDS
