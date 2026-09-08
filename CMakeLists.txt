cmake_minimum_required(VERSION 3.15)

# 1. 设置 C++17 编译标准与优化选项
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(raft_core CXX)

# 2. 查找外部依赖库
find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)
find_package(GTest REQUIRED)
include(GoogleTest)

# 3. 搜集各模块源码文件
file(GLOB_RECURSE LSM_CORE_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/lsmtree/src/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/lsmtree/src/*.cc"
)

file(GLOB PROTOCOL_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/protocol/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/protocol/*.cc"
)

file(GLOB NETWORK_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/network/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/network/*.cc"
)

file(GLOB RAFT_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/raft/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/raft/*.cc"
)

# 4. 定义全局公共头文件路径 (包含 include 目录，供 SDK 引用)
set(COMMON_INCLUDE_DIRS
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/protocol"
    "${CMAKE_CURRENT_SOURCE_DIR}/raft"
    "${CMAKE_CURRENT_SOURCE_DIR}/network"
    "${CMAKE_CURRENT_SOURCE_DIR}/lsmtree/src"
    "${CMAKE_CURRENT_SOURCE_DIR}/lsmtree/third_party"
)

# 5. 编译目标 1：服务端集群节点 (raft_node)
add_executable(raft_node 
    main.cpp
    ${RAFT_SOURCES}
    ${LSM_CORE_SOURCES}
    ${PROTOCOL_SOURCES}
    ${NETWORK_SOURCES}
)
target_include_directories(raft_node PRIVATE ${COMMON_INCLUDE_DIRS})
target_link_libraries(raft_node PRIVATE spdlog::spdlog Threads::Threads)

# 6. 编译目标 2：秒杀业务落地验证程序 (seckill_demo)
# 支持从 example/seckill_demo.cpp 或根目录 seckill_demo.cpp 编译
if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/example/seckill_demo.cpp")
    set(SECKILL_SRC "${CMAKE_CURRENT_SOURCE_DIR}/example/seckill_demo.cpp")
elseif (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/seckill_demo.cpp")
    set(SECKILL_SRC "${CMAKE_CURRENT_SOURCE_DIR}/seckill_demo.cpp")
endif()

if (DEFINED SECKILL_SRC)
    add_executable(seckill_demo 
        ${SECKILL_SRC}
        ${PROTOCOL_SOURCES}
    )
    target_include_directories(seckill_demo PRIVATE ${COMMON_INCLUDE_DIRS})
    target_link_libraries(seckill_demo PRIVATE Threads::Threads spdlog::spdlog)
endif()

# 7. 编译目标 3：客户端 CLI (raft_client_cli)
add_executable(raft_client_cli 
    clientmain.cpp
    ${PROTOCOL_SOURCES}
    ${NETWORK_SOURCES}
)
target_include_directories(raft_client_cli PRIVATE ${COMMON_INCLUDE_DIRS})
target_link_libraries(raft_client_cli PRIVATE spdlog::spdlog Threads::Threads)

# 8. 编译目标 4：RaftLog 单元测试 (raft_log_test)
enable_testing()

add_executable(raft_log_test 
    raft/RaftLog.cpp 
    ${PROTOCOL_SOURCES}
)
target_include_directories(raft_log_test PRIVATE ${COMMON_INCLUDE_DIRS})
target_link_libraries(raft_log_test PRIVATE 
    GTest::gtest
    GTest::gtest_main
    spdlog::spdlog
    Threads::Threads
)

gtest_discover_tests(raft_log_test)