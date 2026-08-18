cmake_minimum_required(VERSION 3.15)

# 1. 设置 C++17 编译标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(raft_core CXX)

# 2. 查找外部依赖库
find_package(Threads REQUIRED) # Linux pthread 线程库
find_package(spdlog REQUIRED)
find_package(GTest REQUIRED)   # GoogleTest 单元测试框架
include(GoogleTest)            # 自动发现测试用例

# 3. 搜集各模块源码文件

# 搜集 LSM-Tree 存储引擎源码
file(GLOB_RECURSE LSM_CORE_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/lsmtree/src/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/lsmtree/src/*.cc"
)

# 搜集 protocol 编解码与 Buffer 源码
file(GLOB PROTOCOL_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/protocol/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/protocol/*.cc"
)

# 搜集 network 网络引擎源码
file(GLOB NETWORK_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/network/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/network/*.cc"
)

# 搜集 raft 共识核心源码 (RaftNode, RaftCore, RaftLog)
file(GLOB RAFT_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/raft/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/raft/*.cc"
)

# 4. 编译目标 1：服务端集群节点 (raft_node)
add_executable(raft_node 
    main.cpp
    ${RAFT_SOURCES}
    ${LSM_CORE_SOURCES}
    ${PROTOCOL_SOURCES}
    ${NETWORK_SOURCES}
)

add_executable(seckill_demo 
    seckill_demo.cpp
    ${PROTOCOL_SOURCES}
)
target_link_libraries(seckill_demo PRIVATE spdlog::spdlog Threads::Threads)

target_include_directories(raft_node PRIVATE 
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/protocol"
    "${CMAKE_CURRENT_SOURCE_DIR}/raft"
    "${CMAKE_CURRENT_SOURCE_DIR}/network"
    "${CMAKE_CURRENT_SOURCE_DIR}/lsmtree/src"
    "${CMAKE_CURRENT_SOURCE_DIR}/lsmtree/third_party"
)

target_link_libraries(raft_node PRIVATE 
    spdlog::spdlog
    Threads::Threads
)

# 5. 编译目标 2：客户端 CLI (raft_client_cli)
add_executable(raft_client_cli 
    clientmain.cpp
    ${PROTOCOL_SOURCES}
    ${NETWORK_SOURCES}
)

target_include_directories(raft_client_cli PRIVATE 
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/protocol"
    "${CMAKE_CURRENT_SOURCE_DIR}/network"
)

target_link_libraries(raft_client_cli PRIVATE 
    spdlog::spdlog
    Threads::Threads
)

# 6. 编译目标 3：RaftLog 单元测试 (raft_log_test)
enable_testing()

add_executable(raft_log_test          # 单元测试入口
    raft/RaftLog.cpp           # 👈 仅引入 RaftLog.cpp，不引入 RaftNode / RaftCore
    ${PROTOCOL_SOURCES}        # 引入 protocol (LogEntry 序列化依赖)
)

target_include_directories(raft_log_test PRIVATE 
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/protocol"
    "${CMAKE_CURRENT_SOURCE_DIR}/raft"
)

target_link_libraries(raft_log_test PRIVATE 
    GTest::gtest
    GTest::gtest_main
    spdlog::spdlog
    Threads::Threads
)

gtest_discover_tests(raft_log_test)