#include "protocol/wire_protocol.h"
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <spdlog/spdlog.h>

std::atomic<int> success_orders{0};
std::atomic<int> failed_orders{0};

bool SendCommand(int fd, raft_node::Opcode op, int32_t key, const std::string& val, std::string& out_reply) {
    std::string packet = raft_node::WireProtocol::Serialize(op, key, val);
    if (::write(fd, packet.data(), packet.size()) != static_cast<ssize_t>(packet.size())) return false;

    char head_buf[raft_node::WireProtocol::kHeaderSize];
    if (::read(fd, head_buf, sizeof(head_buf)) != static_cast<ssize_t>(sizeof(head_buf))) return false;

    auto parsed = raft_node::WireProtocol::ParseHeader(std::string_view(head_buf, sizeof(head_buf)));
    if (!parsed) return false;

    std::string body;
    body.resize(parsed->val_len);
    if (parsed->val_len > 0) {
        if (::read(fd, &body[0], parsed->val_len) != static_cast<ssize_t>(parsed->val_len)) return false;
    }
    out_reply = body;
    return true;
}

int ConnectLeader(const std::string& ip, uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    if (::connect(fd, (sockaddr*)&addr, sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

void BuyerThread(int buyer_id, std::string ip, uint16_t initial_port) {
    uint16_t port = initial_port;
    int fd = -1;

    for (int retry = 0; retry < 5; ++retry) {
        fd = ConnectLeader(ip, port);
        if (fd >= 0) break;
        port = (port == 9883) ? 9881 : port + 1;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    if (fd < 0) return;

    // 发起扣减库存事务请求 (Payload: "BUY:buyer_X")
    std::string reply;
    bool ok = SendCommand(fd, raft_node::Opcode::PUT_RAW, 1001 /* iPhone Key */, "BUY_DECREMENT_STOCK", reply);
    
    if (ok && reply == "OK_COMMIT") {
        success_orders.fetch_add(1);
        spdlog::info("🎉 [买家-{}] 抢购成功！订单已落盘强一致性提交", buyer_id);
    } else {
        failed_orders.fetch_add(1);
    }

    ::close(fd);
}

int main(int argc, char* argv[]) {
    spdlog::set_level(spdlog::level::info);
    spdlog::info("=========================================================================");
    spdlog::info(" 🛍️ 分布式强一致性秒杀业务实战：100 件限量 iPhone 抢购系统点火");
    spdlog::info("=========================================================================");

    int total_buyers = 300; // 300 个人抢 100 件库存
    std::vector<std::thread> buyers;
    buyers.reserve(total_buyers);

    auto start_time = std::chrono::steady_clock::now();

    for (int i = 0; i < total_buyers; ++i) {
        buyers.emplace_back(BuyerThread, i + 1, "127.0.0.1", 9881);
    }

    for (auto& t : buyers) {
        if (t.joinable()) t.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    double cost_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    spdlog::error("=========================================================================");
    spdlog::error(" 秒杀实战战报盘点：");
    spdlog::error(" 1. 成功提交事务总数: {} 单", success_orders.load());
    spdlog::error(" 2. 拦截/售罄/重试  : {} 次", failed_orders.load());
    spdlog::error(" 3. 总体耗时        : {:.2f} ms", cost_ms);
    spdlog::error("=========================================================================");
    return 0;
}