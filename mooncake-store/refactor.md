读取kv.md , GDS\_KV\_Integration\_Analysis.md,  read.md, spec.md, 

gds_interface.h 是C++头文件, 不要把他的Mock实现放在GDSMock类中,直接使用gds_interface.h 暴露的接口。

进行如下两项重构：

1. gds_mock.cpp 的就是真实的语义，但不要封装到NDSMock类中，直接实现gds_interface.h 暴露的接口。

2. 所有使用GDSMock的地方，都改成使用gds_interface.h 暴露的接口。
