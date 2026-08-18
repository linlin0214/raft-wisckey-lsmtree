#include "protocol/wire_protocol.h"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <cerrno>
#include <cstdint>
#include <algorithm>
#include <numeric>
#include <random>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <spdlog/spdlog.h>

std::atomic<uint64_t> total_ops{0};
std::atomic<uint64_t> total_bytes{0};
std::atomic<bool> stop_benchmark{false};

bool write_n(int fd, const char* data, size_t length) {
    size_t written = 0;
    while (written < length) {
        ssize_t n = ::write(fd, data + written, length - written);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                continue;
            }
            return false;
        }
        written += n;
    }
    return true;
}

bool read_n(int fd, char* buf, size_t length) {
    size_t read_bytes = 0;
    while (read_bytes < length) {
        ssize_t n = ::read(fd, buf + read_bytes, length - read_bytes);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                continue;
            }
            return false;
        }
        read_bytes += n;
    }
    return true;
}

void BenchmarkWorker(std::string ip, uint16_t initial_port, int thread_id, 
                     size_t value_size, bool random_key, std::vector<uint32_t>& out_latencies) {
    std::string value_payload(value_size, 'G'); 
    std::vector<char> read_buf(64 * 1024);
    
    uint16_t current_port = initial_port;
    uint32_t local_counter = 0;
    
    std::mt19937 rng(1337 + thread_id);
    std::uniform_int_distribution<uint32_t> dist(1, 100000000);

    out_latencies.reserve(100000);

    while (!stop_benchmark.load(std::memory_order_relaxed)) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        struct sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(current_port);
        ::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

        int buffer_size = 4 * 1024 * 1024;
        ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size));
        ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));

        if (::connect(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            ::close(fd);
            current_port = (current_port == initial_port + 2) ? initial_port : current_port + 1;
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        spdlog::info("[Worker-{}] 成功连接业务节点 {}:{} (Payload: {} B)", 
                     thread_id, ip, current_port, value_size);

        while (!stop_benchmark.load(std::memory_order_relaxed)) {
            uint64_t target_key = random_key ? dist(rng) : (static_cast<uint64_t>(thread_id) * 100000000 + (++local_counter));

            std::string write_packet = raft_node::WireProtocol::Serialize(
                raft_node::Opcode::PUT_RAW, target_key, value_payload
            );

            auto t_start = std::chrono::high_resolution_clock::now();

            if (!write_n(fd, write_packet.data(), write_packet.size())) break;

            const size_t kHeaderSize = raft_node::WireProtocol::kHeaderSize;
            if (!read_n(fd, read_buf.data(), kHeaderSize)) break;

            auto parsed_header = raft_node::WireProtocol::ParseHeader(std::string_view(read_buf.data(), kHeaderSize));
            if (!parsed_header.has_value()) {
                spdlog::error("[Worker-{}] 帧头解包失败，重置连接！", thread_id);
                break;
            }

            uint32_t val_len = parsed_header->val_len;
            if (val_len + kHeaderSize > read_buf.size()) {
                read_buf.resize(val_len + kHeaderSize + 4096);
            }

            if (!read_n(fd, read_buf.data() + kHeaderSize, val_len)) break;

            auto t_end = std::chrono::high_resolution_clock::now();
            uint32_t latency_us = static_cast<uint32_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count()
            );

            std::string_view body_reply(read_buf.data() + kHeaderSize, val_len);

            if (body_reply == "OK_COMMIT") {
                total_ops.fetch_add(1, std::memory_order_relaxed);
                total_bytes.fetch_add(write_packet.size() + kHeaderSize + val_len, std::memory_order_relaxed);
                out_latencies.push_back(latency_us);
            } 
            else if (body_reply.rfind("REJECT", 0) == 0) {
                auto pos = body_reply.find(':');
                if (pos != std::string_view::npos) {
                    std::string port_str(body_reply.substr(pos + 1));
                    try {
                        uint16_t leader_port = static_cast<uint16_t>(std::stoi(port_str));
                        if (leader_port != current_port) {
                            spdlog::warn("[Worker-{}] 非 Leader 节点，重定向至: {} -> {}", 
                                         thread_id, current_port, leader_port);
                            current_port = leader_port;
                        }
                    } catch (...) {}
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                break;
            }
        }

        ::close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cout << "Usage: " << argv[0] << " [Leader_IP] [Port] [Threads] [Duration_Sec] [Value_Size(Default:4096)] [Random_Key(0/1, Default:0)]\n";
        return -1;
    }

    std::string ip = argv[1];
    uint16_t port = static_cast<uint16_t>(std::stoi(argv[2]));
    int thread_count = std::stoi(argv[3]);
    int duration = std::stoi(argv[4]);
    size_t value_size = (argc >= 6) ? static_cast<size_t>(std::stoi(argv[5])) : 4096;
    bool random_key = (argc >= 7) ? (std::stoi(argv[6]) == 1) : false;

    spdlog::set_level(spdlog::level::info);
    spdlog::info("=========================================================================");
    spdlog::info(" 高性能分布式压测轰炸机启动...");
    spdlog::info(" 目标 -> {}:{} | 线程 -> {} | 限时 -> {}s | Payload -> {}B | Key模式 -> {}", 
                 ip, port, thread_count, duration, value_size, random_key ? "随机Key" : "顺序Key");
    spdlog::info("=========================================================================");

    std::vector<std::thread> workers;
    std::vector<std::vector<uint32_t>> thread_latencies(thread_count);
    workers.reserve(thread_count);

    for (int i = 0; i < thread_count; ++i) {
        workers.emplace_back(BenchmarkWorker, ip, port, i, value_size, random_key, std::ref(thread_latencies[i]));
    }

    auto start_time = std::chrono::steady_clock::now();
    uint64_t last_ops = 0;
    uint64_t last_bytes = 0;

    for (int sec = 0; sec < duration; ++sec) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        uint64_t current_ops = total_ops.load(std::memory_order_relaxed);
        uint64_t current_bytes = total_bytes.load(std::memory_order_relaxed);
        
        uint64_t delta_ops = current_ops - last_ops;
        uint64_t delta_bytes = current_bytes - last_bytes;

        double inst_mb_s = static_cast<double>(delta_bytes) / (1024.0 * 1024.0);
        double total_mb = static_cast<double>(current_bytes) / (1024.0 * 1024.0);

        spdlog::info("[Report] 运行第 {:2d}s | 瞬时 QPS: {:6d} ops/s | 瞬时带宽: {:6.2f} MB/s | 累计流量: {:8.2f} MB", 
                     sec + 1, delta_ops, inst_mb_s, total_mb);
        
        last_ops = current_ops;
        last_bytes = current_bytes;
    }

    stop_benchmark.store(true, std::memory_order_release);
    spdlog::warn("[System] 限时到期，等待回收并发线程与收集延迟指标...");
    
    for (auto& t : workers) {
        if (t.joinable()) t.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    double total_elapsed_sec = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() / 1000.0;

    // 安全合并所有线程的延迟数据
    std::vector<uint32_t> all_latencies;
    size_t total_records = 0;
    for (const auto& vec : thread_latencies) {
        total_records += vec.size();
    }
    all_latencies.reserve(total_records);

    for (const auto& vec : thread_latencies) {
        all_latencies.insert(all_latencies.end(), vec.begin(), vec.end());
    }

    spdlog::info("[System] 正在计算 Percentile 长尾延迟指标 (样本数: {})...", all_latencies.size());

    double avg_lat_us = 0;
    uint32_t p50 = 0, p90 = 0, p99 = 0, p999 = 0, max_lat = 0;

    if (!all_latencies.empty()) {
        std::sort(all_latencies.begin(), all_latencies.end());
        size_t n = all_latencies.size();
        
        double sum = std::accumulate(all_latencies.begin(), all_latencies.end(), 0.0);
        avg_lat_us = sum / n;
        
        p50 = all_latencies[static_cast<size_t>(n * 0.50)];
        p90 = all_latencies[static_cast<size_t>(n * 0.90)];
        p99 = all_latencies[static_cast<size_t>(n * 0.99)];
        p999 = all_latencies[static_cast<size_t>(n * 0.999)];
        max_lat = all_latencies.back();
    }

    spdlog::error("=========================================================================");
    spdlog::error(" 压测战报与延迟统计数据盘点：");
    spdlog::error("=========================================================================");
    spdlog::error("1. 成功 Commit 事务总数     : {} 条", total_ops.load());
    spdlog::error("2. 压测总计有效跨度时长     : {:.2f} 秒", total_elapsed_sec);
    spdlog::error("3. 平均端到端 QPS 吞吐流速  : {:.2f} ops/s", static_cast<double>(total_ops.load()) / total_elapsed_sec);
    spdlog::error("4. 平均物理网络传输带宽     : {:.2f} MB/s", (static_cast<double>(total_bytes.load()) / (1024.0 * 1024.0)) / total_elapsed_sec);
    spdlog::error("-------------------------------------------------------------------------");
    spdlog::error("5. 端到端延迟统计 (Latency Distribution):");
    spdlog::error("   - Avg Latency : {:.2f} ms ({:.0f} us)", avg_lat_us / 1000.0, avg_lat_us);
    spdlog::error("   - P50 Latency : {:.2f} ms", p50 / 1000.0);
    spdlog::error("   - P90 Latency : {:.2f} ms", p90 / 1000.0);
    spdlog::error("   - P99 Latency : {:.2f} ms", p99 / 1000.0);
    spdlog::error("   - P999 Latency: {:.2f} ms", p999 / 1000.0);
    spdlog::error("   - Max Latency : {:.2f} ms", max_lat / 1000.0);
    spdlog::error("=========================================================================");

    return 0;
}