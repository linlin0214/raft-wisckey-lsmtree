#pragma once

#include "protocol/wire_protocol.h"
#include "lsmtree/src/Slice.h"
#include <string>
#include <vector>
#include <optional>
#include <chrono>
#include <mutex>
#include <cstring>
#include <cerrno>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace raft {

class RaftClient {
public:
    explicit RaftClient(std::string initial_ip, uint16_t initial_port, int timeout_sec = 3)
        : current_ip_(std::move(initial_ip)), 
          current_port_(initial_port), 
          timeout_sec_(timeout_sec), 
          req_counter_(0), 
          socket_fd_(-1) {}

    ~RaftClient() {
        CloseSocket();
    }

    RaftClient(const RaftClient&) = delete;
    RaftClient& operator=(const RaftClient&) = delete;

    bool Put(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(client_mtx_);
        for (int retry = 0; retry < kMaxRetries; ++retry) {
            uint64_t req_id = ++req_counter_;
            std::string packet = raft_node::WireProtocol::Serialize(
                raft_node::Opcode::PUT_RAW, Slice(key), value, req_id
            );

            auto reply = SendAndRecvUnlocked(packet);
            if (!reply.has_value()) {
                CloseSocket();
                continue;
            }

            // 防御性增强：包含 OK_COMMIT 子串即可判定成功
            if (reply->find("OK_COMMIT") != std::string::npos) {
                return true;
            } else if (HandleRedirectUnlocked(*reply)) {
                continue;
            }
        }
        return false;
    }

    bool Delete(const std::string& key) {
        std::lock_guard<std::mutex> lock(client_mtx_);
        for (int retry = 0; retry < kMaxRetries; ++retry) {
            uint64_t req_id = ++req_counter_;
            std::string packet = raft_node::WireProtocol::Serialize(
                raft_node::Opcode::DEL, Slice(key), "", req_id
            );

            auto reply = SendAndRecvUnlocked(packet);
            if (!reply.has_value()) {
                CloseSocket();
                continue;
            }

            if (reply->find("OK_COMMIT") != std::string::npos) {
                return true;
            } else if (HandleRedirectUnlocked(*reply)) {
                continue;
            }
        }
        return false;
    }

    std::optional<std::string> Get(const std::string& key) {
        std::lock_guard<std::mutex> lock(client_mtx_);
        for (int retry = 0; retry < kMaxRetries; ++retry) {
            uint64_t req_id = ++req_counter_;
            std::string packet = raft_node::WireProtocol::Serialize(
                raft_node::Opcode::GET_RAW, Slice(key), "", req_id
            );

            auto reply = SendAndRecvUnlocked(packet);
            if (!reply.has_value()) {
                CloseSocket();
                continue;
            }

            if (HandleRedirectUnlocked(*reply)) {
                continue;
            }

            if (reply->find("NOT_FOUND") != std::string::npos) {
                return std::nullopt;
            }
            return reply;
        }
        return std::nullopt;
    }

    bool WriteBatch(const std::vector<raft_node::BatchItem>& items) {
        if (items.empty()) return true;

        std::lock_guard<std::mutex> lock(client_mtx_);
        for (int retry = 0; retry < kMaxRetries; ++retry) {
            uint64_t req_id = ++req_counter_;
            std::string packet = raft_node::WireProtocol::SerializeBatch(items, req_id);

            auto reply = SendAndRecvUnlocked(packet);
            if (!reply.has_value()) {
                CloseSocket();
                continue;
            }

            // 防御性增强：包含 OK_COMMIT 子串即可判定成功
            if (reply->find("OK_COMMIT") != std::string::npos) {
                return true;
            } else if (HandleRedirectUnlocked(*reply)) {
                continue;
            }
        }
        return false;
    }

    uint16_t GetCurrentLeaderPort() const {
        std::lock_guard<std::mutex> lock(client_mtx_);
        return current_port_;
    }

private:
    static constexpr int kMaxRetries = 5;

    bool EnsureConnectedUnlocked() {
        if (socket_fd_ >= 0) return true;

        socket_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (socket_fd_ < 0) return false;

        struct timeval tv;
        tv.tv_sec = timeout_sec_;
        tv.tv_usec = 0;
        ::setsockopt(socket_fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ::setsockopt(socket_fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        int buffer_size = 4 * 1024 * 1024;
        ::setsockopt(socket_fd_, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size));
        ::setsockopt(socket_fd_, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));

        struct sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(current_port_);
        ::inet_pton(AF_INET, current_ip_.c_str(), &addr.sin_addr);

        if (::connect(socket_fd_, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            CloseSocket();
            return false;
        }
        return true;
    }

    void CloseSocket() {
        if (socket_fd_ >= 0) {
            ::close(socket_fd_);
            socket_fd_ = -1;
        }
    }

    bool WriteAll(const char* data, size_t len) {
        size_t written = 0;
        while (written < len) {
            ssize_t n = ::send(socket_fd_, data + written, len - written, MSG_NOSIGNAL);
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

    bool ReadAll(char* data, size_t len) {
        size_t read_bytes = 0;
        while (read_bytes < len) {
            ssize_t n = ::read(socket_fd_, data + read_bytes, len - read_bytes);
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

    std::optional<std::string> SendAndRecvUnlocked(const std::string& request_bytes) {
        if (!EnsureConnectedUnlocked()) return std::nullopt;

        if (!WriteAll(request_bytes.data(), request_bytes.size())) {
            CloseSocket();
            return std::nullopt;
        }

        char header_buf[raft_node::WireProtocol::kHeaderSize];
        if (!ReadAll(header_buf, sizeof(header_buf))) {
            CloseSocket();
            return std::nullopt;
        }

        auto parsed_hdr = raft_node::WireProtocol::ParseHeaderWithProbe(
            std::string_view(header_buf, sizeof(header_buf))
        );
        if (!parsed_hdr.has_value()) {
            CloseSocket();
            return std::nullopt;
        }

        uint32_t body_len = parsed_hdr->first.body_len;
        std::string full_packet(sizeof(header_buf) + body_len, '\0');
        std::memcpy(&full_packet[0], header_buf, sizeof(header_buf));

        if (body_len > 0) {
            if (!ReadAll(&full_packet[sizeof(header_buf)], body_len)) {
                CloseSocket();
                return std::nullopt;
            }
        }

        auto full_parsed = raft_node::WireProtocol::Parse(full_packet);
        if (!full_parsed.has_value()) {
            CloseSocket();
            return std::nullopt;
        }

        auto [hdr, ret_key, body_reply, skip] = *full_parsed;
        return std::string(body_reply);
    }

    bool HandleRedirectUnlocked(const std::string& reply) {
        auto pos_reject = reply.find("REJECT");
        if (pos_reject != std::string::npos) {
            auto pos_colon = reply.find(':', pos_reject);
            if (pos_colon != std::string::npos) {
                try {
                    uint16_t leader_port = static_cast<uint16_t>(std::stoi(reply.substr(pos_colon + 1)));
                    if (leader_port > 0 && leader_port != current_port_) {
                        current_port_ = leader_port;
                    }
                } catch (...) {}
            }
            CloseSocket();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            return true;
        }
        return false;
    }

private:
    std::string current_ip_;
    uint16_t current_port_;
    int timeout_sec_;
    uint64_t req_counter_;
    int socket_fd_;
    mutable std::mutex client_mtx_;
};

} // namespace raft