#pragma once
#include "Connection.h"
#include "protocol/Codec.h"
#include <memory>
#include <string>
#include <string_view>
#include <functional>
#include <atomic>

class EventLoop;
class Channel;

class RpcClient : public std::enable_shared_from_this<RpcClient> {
public:
    using ConnectionCompleteCallback = std::function<void(const std::shared_ptr<Connection>&)>;
    using MessageCallback = std::function<void(const std::shared_ptr<Connection>&, Buffer*)>;

private:
    EventLoop* loop_;                           // 所属 EventLoop 句柄
    int fd_{-1};                                // 主动套接字句柄
    std::string peer_ip_;                      // 远端节点 IP
    uint16_t peer_port_;                        // 远端节点端口
    
    std::unique_ptr<Channel> connect_channel_;  // 握手阶段使用的临时 Channel
    std::shared_ptr<Connection> connection_;    // 握手成功后全权托管的 Connection 对象
    
    ConnectionCompleteCallback conn_complete_cb_; // 建连成功通知闭包
    MessageCallback message_callback_;          // 入站 Reply 报文切包回调
    std::atomic<bool> connected_{false};         // 原子建连状态旗标

    void HandleConnect(); // 截获内核三次握手完成

public:
    RpcClient(EventLoop* loop, const std::string& peer_ip, uint16_t peer_port);
    ~RpcClient();

    // 禁用拷贝构造与赋值[cite: 15]
    RpcClient(const RpcClient&) = delete;
    RpcClient& operator=(const RpcClient&) = delete;

    void SetConnectionCompleteCallback(ConnectionCompleteCallback cb) { conn_complete_cb_ = std::move(cb); }
    void SetMessageCallback(MessageCallback cb) { message_callback_ = std::move(cb); }
    
    bool IsConnected() const { return connected_.load(std::memory_order_acquire); }
    std::shared_ptr<Connection> GetConnection() const { return connection_; }

    void Connect();
    void Disconnect();

    //  核心补齐：RPC 语义封装发送接口 (自动完成 Codec::Encode -> Connection::Send)
    void Send(uint8_t opcode, uint64_t req_id, std::string_view body);
};