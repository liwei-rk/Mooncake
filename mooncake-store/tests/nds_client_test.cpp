#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <filesystem>

#include "allocator.h"
#include "client_service.h"
#include "types.h"
#include "utils.h"
#include "test_server_helpers.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");
DEFINE_uint64(default_kv_lease_ttl, mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL,
              "Default lease time for kv objects");
DEFINE_uint32(nsid, 0, "Namespace ID for NDS");

namespace mooncake {
namespace testing {

class NdsClientTest : public ::testing::Test {
   protected:
    static std::shared_ptr<Client> CreateClient(const std::string& host_name) {
        auto client_opt = Client::Create(
            host_name, "P2PHANDSHAKE", FLAGS_protocol,
            std::nullopt, master_address_);
        EXPECT_TRUE(client_opt.has_value())
            << "Failed to create client: " << host_name;
        if (!client_opt.has_value()) return nullptr;
        return client_opt.value();
    }

    static void SetUpTestSuite() {
        FLAGS_logtostderr = 1;

        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
        if (getenv("OD_KV_NSID"))
            FLAGS_nsid = static_cast<uint32_t>(std::stoul(getenv("OD_KV_NSID")));

        if (getenv("DEFAULT_KV_LEASE_TTL")) {
            default_kv_lease_ttl_ = std::stoul(getenv("DEFAULT_KV_LEASE_TTL"));
        } else {
            default_kv_lease_ttl_ = FLAGS_default_kv_lease_ttl;
        }

        tmp_dir_ = std::filesystem::temp_directory_path() /
                   ("mc_nds_test_" + std::to_string(::getpid()));
        std::filesystem::create_directories(tmp_dir_);

        auto config = InProcMasterConfigBuilder()
                          .set_root_fs_dir(tmp_dir_.string())
                          .set_enable_disk_eviction(true)
                          .set_use_od(true)
                          .set_nsid(FLAGS_nsid)
                          .build();
        ASSERT_TRUE(master_.Start(config));
        master_address_ = master_.master_address();

        InitializeClients();
        InitializeSegment();
    }

    static void TearDownTestSuite() {
        CleanupSegment();
        CleanupClients();
        master_.Stop();
        std::error_code ec;
        std::filesystem::remove_all(tmp_dir_, ec);
    }

    static void InitializeClients() {
        buffer_allocator_ = std::make_unique<SimpleAllocator>(256 * 1024 * 1024);

        auto client_opt = Client::Create(
            "localhost:17820", "P2PHANDSHAKE", FLAGS_protocol,
            std::nullopt, master_address_, nullptr, {});
        ASSERT_TRUE(client_opt.has_value())
            << "Failed to create client: localhost:17820";
        client_ = client_opt.value();

        segment_provider_ = CreateClient("localhost:17821");
        ASSERT_TRUE(segment_provider_ != nullptr);

        auto reg_result = client_->RegisterLocalMemory(
            buffer_allocator_->getBase(), 256 * 1024 * 1024, "cpu:0",
            false, false);
        ASSERT_TRUE(reg_result.has_value())
            << "Failed to register local memory";
    }

    static void InitializeSegment() {
        ram_buffer_size_ = 512 * 1024 * 1024;
        segment_ptr_ = allocate_buffer_allocator_memory(ram_buffer_size_);
        ASSERT_TRUE(segment_ptr_);
        auto mount_result = segment_provider_->MountSegment(
            segment_ptr_, ram_buffer_size_, FLAGS_protocol);
        ASSERT_TRUE(mount_result.has_value())
            << "Failed to mount segment";
    }

    static void CleanupSegment() {
        if (segment_provider_ && segment_ptr_) {
            segment_provider_->UnmountSegment(segment_ptr_, ram_buffer_size_);
        }
        if (segment_ptr_) free(segment_ptr_);
    }

    static void CleanupClients() {
        if (buffer_allocator_) buffer_allocator_.reset();
        client_.reset();
        segment_provider_.reset();
    }

    void* AllocateBuffer(size_t size) {
        return buffer_allocator_->allocate(size);
    }

    void DeallocateBuffer(void* ptr, size_t size) {
        buffer_allocator_->deallocate(ptr, size);
    }

    static std::shared_ptr<Client> client_;
    static std::shared_ptr<Client> segment_provider_;
    static std::unique_ptr<SimpleAllocator> buffer_allocator_;
    static void* segment_ptr_;
    static size_t ram_buffer_size_;
    static uint64_t default_kv_lease_ttl_;
    static InProcMaster master_;
    static std::string master_address_;
    static std::filesystem::path tmp_dir_;
};

std::shared_ptr<Client> NdsClientTest::client_ = nullptr;
std::shared_ptr<Client> NdsClientTest::segment_provider_ = nullptr;
std::unique_ptr<SimpleAllocator> NdsClientTest::buffer_allocator_ = nullptr;
void* NdsClientTest::segment_ptr_ = nullptr;
size_t NdsClientTest::ram_buffer_size_ = 0;
uint64_t NdsClientTest::default_kv_lease_ttl_ = 0;
InProcMaster NdsClientTest::master_;
std::string NdsClientTest::master_address_;
std::filesystem::path NdsClientTest::tmp_dir_;

TEST_F(NdsClientTest, PutAndGetSingleKey) {
    const std::string key = "nds_test_put_get";
    const size_t data_size = 1 * 1024 * 1024;

    std::vector<uint8_t> test_data(data_size);
    for (size_t i = 0; i < data_size; ++i) test_data[i] = static_cast<uint8_t>(i & 0xFF);

    void* write_buf = AllocateBuffer(data_size);
    memcpy(write_buf, test_data.data(), data_size);
    std::vector<Slice> write_slices;
    write_slices.emplace_back(Slice{write_buf, data_size});

    ReplicateConfig config;
    config.replica_num = 1;
    auto put_result = client_->Put(key, write_slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Put failed: " << toString(put_result.error());
    DeallocateBuffer(write_buf, data_size);

    void* read_buf = AllocateBuffer(data_size);
    std::vector<Slice> read_slices;
    read_slices.emplace_back(Slice{read_buf, data_size});
    auto get_result = client_->Get(key, read_slices);
    ASSERT_TRUE(get_result.has_value())
        << "Get failed: " << toString(get_result.error());
    ASSERT_EQ(read_slices[0].size, data_size);
    ASSERT_EQ(memcmp(read_slices[0].ptr, test_data.data(), data_size), 0);
    DeallocateBuffer(read_buf, data_size);
}

TEST_F(NdsClientTest, PutAndGetMultiSliceKey) {
    const std::string key = "nds_test_multi_slice";
    const size_t slice1_size = 1 * 1024 * 1024;
    const size_t slice2_size = 2 * 1024 * 1024;
    const size_t total_size = slice1_size + slice2_size;

    std::vector<uint8_t> test_data(total_size);
    for (size_t i = 0; i < test_data.size(); ++i) {
        test_data[i] = static_cast<uint8_t>(i % 256);
    }

    void* write_buf = AllocateBuffer(total_size);
    memcpy(write_buf, test_data.data(), total_size);
    std::vector<Slice> write_slices;
    write_slices.emplace_back(Slice{write_buf, slice1_size});
    write_slices.emplace_back(Slice{static_cast<uint8_t*>(write_buf) + slice1_size, slice2_size});

    ReplicateConfig config;
    config.replica_num = 1;
    auto put_result = client_->Put(key, write_slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Put multi-slice failed: " << toString(put_result.error());
    DeallocateBuffer(write_buf, total_size);

    void* read_buf = AllocateBuffer(total_size);
    std::vector<Slice> read_slices;
    read_slices.emplace_back(Slice{read_buf, slice1_size});
    read_slices.emplace_back(Slice{static_cast<uint8_t*>(read_buf) + slice1_size, slice2_size});

    auto get_result = client_->Get(key, read_slices);
    ASSERT_TRUE(get_result.has_value())
        << "Get multi-slice failed: " << toString(get_result.error());
    ASSERT_EQ(memcmp(read_buf, test_data.data(), total_size), 0);
    DeallocateBuffer(read_buf, total_size);
}

TEST_F(NdsClientTest, BatchPutAndBatchGet) {
    const int batch_size = 10;
    const size_t slice_size = 1 * 1024 * 1024;
    std::vector<std::string> keys;
    std::vector<std::vector<uint8_t>> test_data_list;
    std::vector<std::vector<Slice>> write_slices_list;

    for (int i = 0; i < batch_size; ++i) {
        keys.push_back("nds_batch_key_" + std::to_string(i));
        std::vector<uint8_t> data(slice_size);
        for (size_t j = 0; j < slice_size; ++j) data[j] = static_cast<uint8_t>((i + j) & 0xFF);
        test_data_list.push_back(data);
    }

    for (int i = 0; i < batch_size; ++i) {
        void* buf = AllocateBuffer(slice_size);
        memcpy(buf, test_data_list[i].data(), slice_size);
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buf, slice_size});
        write_slices_list.push_back(std::move(slices));
    }

    ReplicateConfig config;
    config.replica_num = 1;
    auto batch_put_results = client_->BatchPut(keys, write_slices_list, config);
    for (size_t i = 0; i < batch_put_results.size(); ++i) {
        ASSERT_TRUE(batch_put_results[i].has_value())
            << "BatchPut[" << i << "] failed: "
            << toString(batch_put_results[i].error());
    }

    for (int i = 0; i < batch_size; ++i) {
        DeallocateBuffer(write_slices_list[i][0].ptr, slice_size);
    }

    std::unordered_map<std::string, std::vector<Slice>> read_slices_map;
    for (int i = 0; i < batch_size; ++i) {
        void* buf = AllocateBuffer(slice_size);
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buf, slice_size});
        read_slices_map[keys[i]] = std::move(slices);
    }

    auto batch_get_results = client_->BatchGet(keys, read_slices_map);
    for (size_t i = 0; i < batch_get_results.size(); ++i) {
        ASSERT_TRUE(batch_get_results[i].has_value())
            << "BatchGet[" << i << "] failed: "
            << toString(batch_get_results[i].error());
    }

    for (int i = 0; i < batch_size; ++i) {
        const auto& slices = read_slices_map[keys[i]];
        ASSERT_EQ(slices.size(), 1);
        ASSERT_EQ(memcmp(slices[0].ptr, test_data_list[i].data(),
                         slice_size), 0);
        DeallocateBuffer(slices[0].ptr, slice_size);
    }
}

TEST_F(NdsClientTest, BatchPutThenIndividualGet) {
    const int batch_size = 5;
    const size_t slice_size = 2 * 1024 * 1024;
    std::vector<std::string> keys;
    std::vector<std::vector<uint8_t>> test_data_list;
    std::vector<std::vector<Slice>> write_slices_list;

    for (int i = 0; i < batch_size; ++i) {
        keys.push_back("nds_batch_then_single_" + std::to_string(i));
        std::vector<uint8_t> data(slice_size);
        for (size_t j = 0; j < slice_size; ++j) data[j] = static_cast<uint8_t>((i * 3 + j) & 0xFF);
        test_data_list.push_back(data);
    }

    for (int i = 0; i < batch_size; ++i) {
        void* buf = AllocateBuffer(slice_size);
        memcpy(buf, test_data_list[i].data(), slice_size);
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buf, slice_size});
        write_slices_list.push_back(std::move(slices));
    }

    ReplicateConfig config;
    config.replica_num = 1;
    auto batch_put_results = client_->BatchPut(keys, write_slices_list, config);
    for (size_t i = 0; i < batch_put_results.size(); ++i) {
        ASSERT_TRUE(batch_put_results[i].has_value())
            << "BatchPut[" << i << "] failed";
    }
    for (int i = 0; i < batch_size; ++i) {
        DeallocateBuffer(write_slices_list[i][0].ptr, slice_size);
    }

    for (int i = 0; i < batch_size; ++i) {
        void* buf = AllocateBuffer(slice_size);
        std::vector<Slice> read_slices;
        read_slices.emplace_back(Slice{buf, slice_size});

        auto get_result = client_->Get(keys[i], read_slices);
        ASSERT_TRUE(get_result.has_value())
            << "Get after BatchPut[" << i << "] failed: "
            << toString(get_result.error());
        ASSERT_EQ(memcmp(read_slices[0].ptr, test_data_list[i].data(),
                         slice_size), 0);
        DeallocateBuffer(buf, slice_size);
    }
}

TEST_F(NdsClientTest, IndividualPutThenBatchGet) {
    const int batch_size = 5;
    const size_t slice_size = 2 * 1024 * 1024;
    std::vector<std::string> keys;
    std::vector<std::vector<uint8_t>> test_data_list;

    for (int i = 0; i < batch_size; ++i) {
        keys.push_back("nds_single_then_batch_" + std::to_string(i));
        std::vector<uint8_t> data(slice_size);
        for (size_t j = 0; j < slice_size; ++j) data[j] = static_cast<uint8_t>((i * 7 + j) & 0xFF);
        test_data_list.push_back(data);
    }

    ReplicateConfig config;
    config.replica_num = 1;
    for (int i = 0; i < batch_size; ++i) {
        void* buf = AllocateBuffer(slice_size);
        memcpy(buf, test_data_list[i].data(), slice_size);
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buf, slice_size});

        auto put_result = client_->Put(keys[i], slices, config);
        ASSERT_TRUE(put_result.has_value())
            << "Put[" << i << "] failed: " << toString(put_result.error());
        DeallocateBuffer(buf, slice_size);
    }

    std::unordered_map<std::string, std::vector<Slice>> read_slices_map;
    for (int i = 0; i < batch_size; ++i) {
        void* buf = AllocateBuffer(slice_size);
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buf, slice_size});
        read_slices_map[keys[i]] = std::move(slices);
    }

    auto batch_get_results = client_->BatchGet(keys, read_slices_map);
    for (size_t i = 0; i < batch_get_results.size(); ++i) {
        ASSERT_TRUE(batch_get_results[i].has_value())
            << "BatchGet after Put[" << i << "] failed: "
            << toString(batch_get_results[i].error());
    }

    for (int i = 0; i < batch_size; ++i) {
        const auto& slices = read_slices_map[keys[i]];
        ASSERT_EQ(memcmp(slices[0].ptr, test_data_list[i].data(),
                         slice_size), 0);
        DeallocateBuffer(slices[0].ptr, slice_size);
    }
}

TEST_F(NdsClientTest, BatchPutWithMultiSlicePerKey) {
    const int batch_size = 3;
    const size_t slice_sizes[] = {1 * 1024 * 1024, 2 * 1024 * 1024};
    std::vector<std::string> keys;
    std::vector<std::vector<uint8_t>> test_data_list;
    std::vector<std::vector<Slice>> write_slices_list;

    for (int i = 0; i < batch_size; ++i) {
        keys.push_back("nds_batch_multi_slice_" + std::to_string(i));
        size_t total = slice_sizes[0] + slice_sizes[1];
        std::vector<uint8_t> data(total);
        for (size_t j = 0; j < total; ++j) data[j] = static_cast<uint8_t>((i + j) & 0xFF);
        test_data_list.push_back(data);
    }

    for (int i = 0; i < batch_size; ++i) {
        size_t total = slice_sizes[0] + slice_sizes[1];
        void* buf = AllocateBuffer(total);
        memcpy(buf, test_data_list[i].data(), total);
        std::vector<Slice> slices;
        slices.emplace_back(Slice{buf, slice_sizes[0]});
        slices.emplace_back(Slice{static_cast<uint8_t*>(buf) + slice_sizes[0], slice_sizes[1]});
        write_slices_list.push_back(std::move(slices));
    }

    ReplicateConfig config;
    config.replica_num = 1;
    auto batch_put_results = client_->BatchPut(keys, write_slices_list, config);
    for (size_t i = 0; i < batch_put_results.size(); ++i) {
        ASSERT_TRUE(batch_put_results[i].has_value())
            << "BatchPut multi-slice[" << i << "] failed";
    }

    for (int i = 0; i < batch_size; ++i) {
        size_t total = slice_sizes[0] + slice_sizes[1];
        DeallocateBuffer(write_slices_list[i][0].ptr, total);
    }

    std::unordered_map<std::string, std::vector<Slice>> read_slices_map;
    for (int i = 0; i < batch_size; ++i) {
        size_t total = slice_sizes[0] + slice_sizes[1];
        void* rbuf = AllocateBuffer(total);
        std::vector<Slice> slices;
        slices.emplace_back(Slice{rbuf, slice_sizes[0]});
        slices.emplace_back(Slice{static_cast<uint8_t*>(rbuf) + slice_sizes[0], slice_sizes[1]});
        read_slices_map[keys[i]] = std::move(slices);
    }

    auto batch_get_results = client_->BatchGet(keys, read_slices_map);
    for (size_t i = 0; i < batch_get_results.size(); ++i) {
        ASSERT_TRUE(batch_get_results[i].has_value())
            << "BatchGet multi-slice[" << i << "] failed";
    }

    for (int i = 0; i < batch_size; ++i) {
        const auto& slices = read_slices_map[keys[i]];
        ASSERT_EQ(slices.size(), 2);
        size_t total = slice_sizes[0] + slice_sizes[1];
        ASSERT_EQ(memcmp(slices[0].ptr, test_data_list[i].data(),
                         total), 0);
        DeallocateBuffer(slices[0].ptr, total);
    }
}

TEST_F(NdsClientTest, OverwriteExistingKey) {
    const std::string key = "nds_test_overwrite";
    const size_t v1_size = 1 * 1024 * 1024;
    const size_t v2_size = 2 * 1024 * 1024;

    std::vector<uint8_t> data_v1(v1_size);
    std::vector<uint8_t> data_v2(v2_size);
    for (size_t i = 0; i < v1_size; ++i) data_v1[i] = static_cast<uint8_t>(i & 0xFF);
    for (size_t i = 0; i < v2_size; ++i) data_v2[i] = static_cast<uint8_t>((i + 0x55) & 0xFF);

    ReplicateConfig config;
    config.replica_num = 1;

    void* buf = AllocateBuffer(v1_size);
    memcpy(buf, data_v1.data(), v1_size);
    std::vector<Slice> slices;
    slices.emplace_back(Slice{buf, v1_size});
    auto put1 = client_->Put(key, slices, config);
    ASSERT_TRUE(put1.has_value()) << "Put v1 failed";
    DeallocateBuffer(buf, v1_size);

    auto remove_result = client_->Remove(key);
    ASSERT_TRUE(remove_result.has_value()) << "Remove v1 failed";

    buf = AllocateBuffer(v2_size);
    memcpy(buf, data_v2.data(), v2_size);
    slices.clear();
    slices.emplace_back(Slice{buf, v2_size});
    auto put2 = client_->Put(key, slices, config);
    ASSERT_TRUE(put2.has_value()) << "Put v2 (overwrite) failed";
    DeallocateBuffer(buf, v2_size);

    void* read_buf = AllocateBuffer(v2_size);
    slices.clear();
    slices.emplace_back(Slice{read_buf, v2_size});
    auto get_result = client_->Get(key, slices);
    ASSERT_TRUE(get_result.has_value()) << "Get after overwrite failed";
    ASSERT_EQ(memcmp(read_buf, data_v2.data(), v2_size), 0);
    DeallocateBuffer(read_buf, v2_size);
}

TEST_F(NdsClientTest, GetNonExistentKey) {
    const std::string key = "nds_key_does_not_exist";
    const size_t buf_size = 1 * 1024 * 1024;
    void* buf = AllocateBuffer(buf_size);
    std::vector<Slice> slices;
    slices.emplace_back(Slice{buf, buf_size});
    auto get_result = client_->Get(key, slices);
    ASSERT_FALSE(get_result.has_value());
    DeallocateBuffer(buf, buf_size);
}

TEST_F(NdsClientTest, LargePayloadPutAndGet) {
    const std::string key = "nds_test_large_payload";
    const size_t payload_size = 16 * 1024 * 1024;

    std::vector<uint8_t> payload(payload_size);
    for (size_t i = 0; i < payload_size; ++i) {
        payload[i] = static_cast<uint8_t>(i & 0xFF);
    }

    void* write_buf = AllocateBuffer(payload_size);
    memcpy(write_buf, payload.data(), payload_size);
    std::vector<Slice> write_slices;
    write_slices.emplace_back(Slice{write_buf, payload_size});

    ReplicateConfig config;
    config.replica_num = 1;
    auto put_result = client_->Put(key, write_slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Put large payload failed: " << toString(put_result.error());
    DeallocateBuffer(write_buf, payload_size);

    void* read_buf = AllocateBuffer(payload_size);
    std::vector<Slice> read_slices;
    read_slices.emplace_back(Slice{read_buf, payload_size});
    auto get_result = client_->Get(key, read_slices);
    ASSERT_TRUE(get_result.has_value())
        << "Get large payload failed: " << toString(get_result.error());
    ASSERT_EQ(memcmp(read_buf, payload.data(), payload_size), 0);
    DeallocateBuffer(read_buf, payload_size);
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    google::InitGoogleLogging("NdsClientTest");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    testing::InitGoogleTest(&argc, argv);
    auto result = RUN_ALL_TESTS();
    google::ShutdownGoogleLogging();
    return result;
}