读取kv.md , GDS\_KV\_Integration\_Analysis.md,  read.md, spec.md, 

gds_interface.h 是C++头文件, 不要把他的Mock实现放在NDSMock类中,直接使用gds_interface.h 暴露的接口。

进行如下两项重构：

1. gds_mock.cpp 的就是真实的语义，但不要封装到NDSMock类中，直接实现gds_interface.h 暴露的接口。如果已经实现了这项重构，请忽略这一项。


3. 现在include/gds/gds_mock.h 该名字未gds_mock_test_utils.h,gds_mock_utils.cpp 改名未gds_mock_test_utils.cpp, 改完后 确保test_gds_mock.cpp 能够通过make 编译
