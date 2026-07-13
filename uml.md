
```
@startuml
autonumber

actor "Python User" as Py
participant "Store" as Store
participant "Client" as Client
participant "Master\n(RPC)" as Master
participant "KVStorageBackend" as KVSB
participant "NDSLoader" as NDSL
participant "libndskv.so\n(dlopen)" as Lib

note over Py : store.setup(use_od=True)

Py -> Store : setup(use_od=True)
Store -> Client : Create()

Client -> Master : GetStorageConfig()
Master --> Client : use_od=true
note right of Client : use_od_=true

Client -> KVSB : PrepareStorageBackend()\n(创建，不调 Init)
Client -> KVSB : setNsid(nsid_=1)
note right of KVSB : nsid_=1\ninitialized_=false

Py -> Store : register_buffer(ptr, size)
Store -> Client : RegisterLocalMemory(ptr, size)

note over Client
  condition: use_od_=true
  && kv_storage_backend_
  && !isInitialized()
end note

Client -> KVSB : Init(ptr, size)

KVSB -> NDSL : Load()
NDSL -> Lib : dlopen("libndskv.so")
Lib --> NDSL : handle

NDSL -> Lib : dlsym("c_init")
NDSL -> Lib : dlsym("c_get")
NDSL -> Lib : dlsym("c_put")
NDSL -> Lib : dlsym("c_batchGet")
NDSL -> Lib : dlsym("c_batchPut")
NDSL -> Lib : dlsym("c_isExists")
Lib --> NDSL : function pointers

note over NDSL : MC_NDS_CONFIG env\n→ nds_config_path\n(default: nds_config.conf)

KVSB -> NDSL : init(ptr, size, nds_config_path)
NDSL -> Lib : c_init(ptr, size, nds_config_path)
Lib --> NDSL : return code
NDSL --> KVSB : init done

KVSB -> KVSB : initialized_ = true
@enduml
```

···
@startuml NDS Cleanup Flow
autonumber

actor "Python User" as Py
participant "Store" as Store
participant "Client" as Client
participant "KVStorageBackend" as KVSB
participant "NDSLoader" as NDSL

Py -> Store : unregister_buffer(ptr)
Store -> Client : unregisterLocalMemory(ptr)

note over Client
  condition: use_od_=true
  && kv_storage_backend_
  && isInitialized()
end note

Client -> KVSB : CleanupNDS()

alt owns_nds_memory_ == true
  KVSB -> KVSB : free(nds_mem_addr_)
  note right of KVSB : owns_nds_memory_=true\n自分配内存需释放
else owns_nds_memory_ == false
  note right of KVSB : 外部提供内存\n不负责释放
end

KVSB -> NDSL : 重置全局状态
NDSL -> NDSL : 清除 dlopen handle\n清除 dlsym function pointers

KVSB -> KVSB : initialized_ = false

@enduml
···
