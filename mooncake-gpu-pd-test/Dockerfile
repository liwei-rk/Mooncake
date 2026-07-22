# Dockerfile for Mooncake PD Disaggregated Serving on NVIDIA GPU
# Base: vLLM image (includes CUDA, Python, vLLM)
# Adds: Mooncake (CUDA build), LMCache, PD test scripts
#
# !!! 构建方式 !!!
#   cd mooncake-gpu-pd-test
#   docker build -t mooncake-pd-gpu -f Dockerfile .
#   (build context = mooncake-gpu-pd-test/ 目录本身)
#
# !!! 运行方式 !!!
#   docker run --gpus all --network host \
#     -v /mnt/model:/mnt/model \
#     -v $(pwd):/workspace/pd-test \
#     mooncake-pd-gpu

FROM vllm/vllm-openai:v0.21.0

# ===================================================================
# 为什么用 vllm/vllm-openai 做base？
# 因为它已经装好了 CUDA 12.x + Python + vLLM + PyTorch + CUDA kernels
# 我们只需要在上面加 Mooncake 和 LMCache，省去大量重复工作
#
# !!! 注意：vllm/vllm-openai:latest 里 python 命令是 python3 !!!
# 容器内用 python 或 python3 都可以（有 python-is-python3 别名）
# ===================================================================

# --- Mooncake 构建依赖 ---
# !!! 为什么需要这些？ !!!
# cmake: 构建工具
# libibverbs-dev: RDMA verbs 库（TCP 模式也装着，不影响）
# libgoogle-glog-dev: 日志库
# libgrpc-dev: gRPC（Mooncake master 通信）
# pybind11-dev: Python C++ 绑定
# libcurl4-openssl-dev: HTTP client（metadata server 通信）
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake build-essential git wget curl \
    libibverbs-dev libunwind-dev libgoogle-glog-dev libgtest-dev \
    libjsoncpp-dev libnuma-dev libboost-all-dev libssl-dev \
    libgrpc-dev libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc \
    pybind11-dev libcurl4-openssl-dev libhiredis-dev \
    libyaml-cpp-dev libjemalloc-dev pkg-config patchelf \
    && rm -rf /var/lib/apt/lists/*

# --- Go (Mooncake metadata server 是 Go 写的) ---
# !!! 用中国镜像加速 !!!
RUN wget -q https://mirrors.aliyun.com/golang/go1.22.12.linux-amd64.tar.gz -O go.tar.gz \
    || wget -q https://go.dev/dl/go1.22.12.linux-amd64.tar.gz -O go.tar.gz \
    && tar -C /usr/local -xzf go.tar.gz \
    && rm go.tar.gz
ENV PATH=/usr/local/go/bin:$PATH
ENV GOPROXY=https://goproxy.cn

# --- yalantinglibs (Mooncake 的 C++ 依赖) ---
# !!! 用 GitHub 镜像加速 !!!
RUN git clone --depth 1 https://github.com/alibaba/yalantinglibs.git /tmp/yalantinglibs \
    || git clone --depth 1 https://ghfast.top/https://github.com/alibaba/yalantinglibs.git /tmp/yalantinglibs \
    && cd /tmp/yalantinglibs && mkdir -p build && cd build \
    && cmake .. -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF -DBUILD_UNIT_TESTS=OFF \
    && cmake --build . -j$(nproc) \
    && cmake --install . \
    && rm -rf /tmp/yalantinglibs

# --- 编译 Mooncake with CUDA ---
# !!! 关键: -DUSE_CUDA=ON 启用 GPU 支持 !!!
# 如果用 RDMA 还需要 -DUSE_RDMA=ON (需要 MOFED 驱动)
# 我们先用 TCP，验证通过后再切 RDMA
#
# !!! 为什么用 git clone 而不是 COPY? !!!
# 因为 build context 只有 mooncake-gpu-pd-test/ 目录，不包含 Mooncake 源码
# 如果你有本地修改的 Mooncake 源码，运行时用 -v 挂载覆盖
RUN git clone https://github.com/kvcache-ai/Mooncake.git /opt/mooncake-src \
    || git clone https://ghfast.top/https://github.com/kvcache-ai/Mooncake.git /opt/mooncake-src \
    && cd /opt/mooncake-src && mkdir -p build && cd build \
    && cmake -DUSE_CUDA=ON -DBUILD_EXAMPLES=ON .. \
    && make -j$(nproc) \
    && make install \
    && ldconfig

# --- 安装 Mooncake Python bindings ---
# nds_stress_test.py 里 from mooncake.store import MooncakeDistributedStore 依赖这个
RUN cd /opt/mooncake-src/mooncake-integration \
    && pip install --no-cache-dir -e .

# --- 安装 LMCache ---
# PD分离测试里 LMCacheConnectorV1 依赖这个包
# LMCache 是 vLLM 的 KV cache 管理层，负责和 Mooncake 对接
RUN pip install --no-cache-dir lmcache

# --- 安装 PD Proxy 依赖 ---
# mooncake_pd_proxy.py 需要 fastapi + uvicorn + httpx
RUN pip install --no-cache-dir fastapi uvicorn httpx

# --- 工作目录 ---
WORKDIR /workspace/pd-test

# 默认启动 bash
CMD ["bash"]
