#pragma once

#include "gds_interface.h"
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace NDS {

// Mock data storage for testing
class NDSMock {
public:
    static NDSMock& instance();
    
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
    NDSMock() = default;
    ~NDSMock() = default;
    NDSMock(const NDSMock&) = delete;
    NDSMock& operator=(const NDSMock&) = delete;
    
    mutable std::mutex mutex_;
    bool initialized_ = false;
    
    // Helper method to get filename from blockId
    std::string getBlockFilename(uint64_t blockId) const;
};

} // namespace NDS
