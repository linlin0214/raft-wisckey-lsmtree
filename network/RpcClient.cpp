#include "RpcClient.h"
#include "EventLoop.h"
#include "Channel.h"
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

RpcClient::RpcClient(EventLoop* loop, const std::string& peer_ip, uint16_t peer_port)
    : loop_(loop), fd_(-1), peer_ip_(peer_ip), peer_port_(peer_port) {}

RpcClient::~RpcClient() {
    loop_->AssertInLoopThread();
    if (connect_channel_) {
        connect_channel_->DisableAll();
        connect_channel_->Remove();
        connect_channel_.reset();
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void RpcClient::Connect() {
    loop_->AssertInLoopThread();
    if (connected_.load(std::memory_order_acquire) || connect_channel_ != nullptr) return;

    fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd_ < 0) {
        spdlog::error("[RpcClient] 创建主动套接字失败！errno: {}", errno);
        return;
    }

    int opt = 1;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer_port_);
    ::inet_pton(AF_INET, peer_ip_.c_str(), &addr.sin_addr);

    int ret = ::connect(fd_, (struct sockaddr*)&addr, sizeof(addr));
    
    if (ret == 0) {
        HandleConnect();
    } else if (ret < 0 && errno == EINPROGRESS) {
        connect_channel_ = std::make_unique<Channel>(loop_, fd_);
        connect_channel_->Tie(shared_from_this());

        std::weak_ptr<RpcClient> weak_self = shared_from_this();
        connect_channel_->SetWriteCallback([weak_self]() {
            if (auto self = weak_self.lock()) self->HandleConnect();
        });
        connect_channel_->SetErrorCallback([weak_self]() {
            if (auto self = weak_self.lock()) self->HandleConnect();
        }); 
        connect_channel_->EnableWriting(); 
    } else {
        spdlog::warn("[RpcClient] 连接遭到内核拒绝！远端 {}:{} 端口可能尚未开放。errno: {}", peer_ip_, peer_port_, errno);
        ::close(fd_);
        fd_ = -1;
    }
}

void RpcClient::HandleConnect() {
    loop_->AssertInLoopThread();

    if (connect_channel_) {
        connect_channel_->DisableAll();
        connect_channel_->Remove();
        std::shared_ptr<Channel> ch = std::move(connect_channel_);
        loop_->QueueInLoop([ch]() {});
    }

    int err = 0;
    socklen_t len = sizeof(err);
    int ret = ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len);
    
    if (ret < 0 || err != 0) {
        spdlog::warn("[RpcClient] 远端节点 {}:{} 尚未开门，安全清理 fd...", peer_ip_, peer_port_);
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        return;
    }

    spdlog::info("[RpcClient] 异步 TCP 三次握手闭合！成功击穿对端节点守护大门 {}:{}", peer_ip_, peer_port_);

    connection_ = std::make_shared<Connection>(loop_, fd_);
    fd_ = -1;
    connected_.store(true, std::memory_order_release);

    //  重点补齐：绑定入站消息回调，使对端回复的 Reply 报文能被 Dispatcher 正常接收
    if (message_callback_) {
        connection_->SetMessageCallback(message_callback_);
    }

    std::weak_ptr<RpcClient> weak_self = shared_from_this();
    connection_->SetCloseCallback([weak_self](const std::shared_ptr<Connection>& conn) {
        if (auto self = weak_self.lock()) {
            self->loop_->AssertInLoopThread();
            spdlog::warn("[RpcClient] 侦测到 TCP 链路断开 {}:{}，更新连接状态为 false", self->peer_ip_, self->peer_port_);
            self->connected_.store(false, std::memory_order_release);
            self->connection_.reset();
        }
    });

    connection_->ConnectionEstablished();

    if (conn_complete_cb_) {
        conn_complete_cb_(connection_);
    }
}

void RpcClient::Disconnect() {
    loop_->AssertInLoopThread();
    if (connection_) {
        connection_->ConnectionDestroyed();
        connection_.reset();
    }
    connected_.store(false, std::memory_order_release);
}

// 将 RPC 请求自动进行 Codec 打包并投递给 Connection 传输层
void RpcClient::Send(uint8_t opcode, uint64_t req_id, std::string_view body) {
    // 1. 在调用线程安全地完成二进制打包 (生成独立的 std::string)
    std::string packed = raft_rpc::Codec::Encode(opcode, req_id, body);

    // 2. 局部快照获取 connection_ 存根，防止竞态重置
    std::shared_ptr<Connection> conn = connection_;

    if (!conn || !conn->IsConnected()) {
        spdlog::debug("[RpcClient] 尝试向未建连对端 {}:{} 发送 RPC (opcode: {})，消息丢弃", 
                      peer_ip_, peer_port_, static_cast<int>(opcode));
        return;
    }

    // 3.  跨线程安全跳跃：强制投递给所属的 EventLoop 线程执行 Connection::Send
    loop_->RunInLoop([conn, packed = std::move(packed)]() {
        if (conn->IsConnected()) {
            conn->Send(packed);
        }
    });
}