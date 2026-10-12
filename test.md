1. 启动元数据服务器：
mooncake_http_metadata_server --port 8080 &
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata
2. 启动 master server（需要 use_od=true 配置以启用NDS路径）
3. 放置 key.txt 到脚本运行目录（项目根目录），每行一个key字符串
4. 运行脚本：
# 只测 batch_put，128KB block，128 keys/batch，8线程，30秒
python mooncake-store/tests/nds_stress_test.py --operation-mode batch_put --block-size 131072 --batch-size 128 --num-threads 8 --duration 30
# 只测 batch_get
python mooncake-store/tests/nds_stress_test.py --operation-mode batch_get --block-size 131072 --batch-size 128
# 混合读写（一半线程put，一半线程get）
python mooncake-store/tests/nds_stress_test.py --operation-mode mixed --num-threads 8
# RDMA模式
python mooncake-store/tests/nds_stress_test.py --protocol rdma --device-name erdma_0 --local-hostname <hostname>:<port>
# 指定key.txt路径
python mooncake-store/tests/nds_stress_test.py --key-file /path/to/key.txt
关键前提：master server 必须配置 use_od=true，否则客户端不会走 KVStorageBackend（NDS）路径，而是走 StorageBackend（文件系统）路径。