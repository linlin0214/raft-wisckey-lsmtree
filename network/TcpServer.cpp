#include "TcpServer.h"
#include "EventLoop.h"
#include "Acceptor.h"
#include <spdlog/spdlog.h>

TcpServer::TcpServer(EventLoop* loop, const std::string& ip, uint16_t port)
    : loop_(loop),
      acceptor_(std::make_unique<Acceptor>(loop, ip, port)) {
    
    acceptor_->SetNewConnectionCallback([this](int sockfd) {
        this->NewConnection(sockfd);
    });
}

TcpServer::~TcpServer() {
    loop_->AssertInLoopThread();
    spdlog::warn("[TcpServer] 服务器正在析构，开始安全清场所有连接存根...");
    
    // 1. 优先销毁 Acceptor，立刻拔除 listen_fd
    acceptor_.reset();

    // 清场时解绑 close_callback_，防止已析构的 TcpServer 被回调
    for (auto& item : connections_) {
        std::shared_ptr<Connection> conn = item.second;
        if (conn) {
            conn->SetCloseCallback(nullptr);
            conn->ConnectionDestroyed();
        }
    }
    connections_.clear();
}

void TcpServer::Start() {
    if (!started_.exchange(true)) {
        loop_->RunInLoop([this]() {
            spdlog::info("[TcpServer] 统一网络服务大盘正式切入高并发就绪流。");
            if (acceptor_) {
                acceptor_->Listen();
            }
        });
    }
}

void TcpServer::NewConnection(int sockfd) {
    loop_->AssertInLoopThread();
    
    // 1. 孵化 Connection 对象
    auto conn = std::make_shared<Connection>(loop_, sockfd);
    connections_[sockfd] = conn;

    // 2. 注入业务层切包回调与销毁闭包
    conn->SetMessageCallback(message_callback_);
    conn->SetCloseCallback([this](const std::shared_ptr<Connection>& c) {
        this->RemoveConnection(c);
    });

    // 3. 挂载 Epoll，开启 EPOLLIN 监听
    conn->ConnectionEstablished();

    // 4. 通知上层应用
    if (connection_callback_) {
        connection_callback_(conn);
    }
}

void TcpServer::RemoveConnection(const std::shared_ptr<Connection>& conn) {
    // HandleClose 已在 Loop 线程异步 Queue 调度，此处直接在 Loop 线程同步执行清理，消灭二次 Queue
    loop_->AssertInLoopThread();
    
    spdlog::info("[TcpServer] 安全注销 connections_ Map 中的 fd 存根: {}", conn->GetFd());
    
    auto it = connections_.find(conn->GetFd());
    if (it != connections_.end() && it->second == conn) {
        connections_.erase(it);
    }
    
    // 同步解绑 Epoll 并完成资源回收
    conn->ConnectionDestroyed();
}