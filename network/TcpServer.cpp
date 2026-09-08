#include "TcpServer.h"
#include "EventLoop.h"
#include "Acceptor.h"
#include "EventLoopThreadPool.h"
#include <spdlog/spdlog.h>
#include <cassert>

TcpServer::TcpServer(EventLoop* loop, const std::string& ip, uint16_t port)
    : loop_(loop),
      acceptor_(std::make_unique<Acceptor>(loop, ip, port)),
      thread_pool_(std::make_unique<EventLoopThreadPool>(loop)) {
    
    acceptor_->SetNewConnectionCallback([this](int sockfd) {
        this->NewConnection(sockfd);
    });
}

TcpServer::~TcpServer() {
    loop_->AssertInLoopThread();
    spdlog::warn("[TcpServer] 服务器正在析构，开始安全清场所有连接存根...");
    
    acceptor_.reset();

    for (auto& item : connections_) {
        std::shared_ptr<Connection> conn = item.second;
        if (conn) {
            conn->SetCloseCallback(nullptr);
            EventLoop* io_loop = conn->GetLoop();
            io_loop->QueueInLoop([conn]() {
                conn->ConnectionDestroyed();
            });
        }
    }
    connections_.clear();
}

void TcpServer::SetThreadNum(int num_threads) {
    assert(num_threads >= 0);
    thread_pool_->SetThreadNum(num_threads);
}

void TcpServer::Start() {
    if (!started_.exchange(true)) {
        thread_pool_->Start(); // 启动从属 I/O 线程池
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
    
    // 从线程池获取一个从属 EventLoop
    EventLoop* io_loop = thread_pool_->GetNextLoop();

    // 将物理套接字绑定在该 io_loop 上
    auto conn = std::make_shared<Connection>(io_loop, sockfd);
    connections_[sockfd] = conn;

    conn->SetMessageCallback(message_callback_);
    conn->SetCloseCallback([this](const std::shared_ptr<Connection>& c) {
        this->RemoveConnection(c);
    });

    // 跨线程调度至专属的 Sub-Reactor 挂载 Epoll
    io_loop->RunInLoop([conn, this]() {
        conn->ConnectionEstablished();
        if (this->connection_callback_) {
            this->connection_callback_(conn);
        }
    });
}

void TcpServer::RemoveConnection(const std::shared_ptr<Connection>& conn) {
    // 强制跳转回 Main Reactor 线程清理 connections_ 表
    loop_->RunInLoop([this, conn]() {
        this->RemoveConnectionInLoop(conn);
    });
}

void TcpServer::RemoveConnectionInLoop(const std::shared_ptr<Connection>& conn) {
    loop_->AssertInLoopThread();
    
    auto it = connections_.find(conn->GetFd());
    if (it != connections_.end() && it->second == conn) {
        connections_.erase(it);
    }
    
    // 连接的拔除操作必须在所属 Sub-Reactor 线程执行
    EventLoop* io_loop = conn->GetLoop();
    io_loop->QueueInLoop([conn]() {
        conn->ConnectionDestroyed();
    });
}

void TcpServer::Stop() {
    loop_->AssertInLoopThread();
    spdlog::info("[TcpServer] 停止监听，注销迎宾套接字并切断现有连接...");
    
    if (acceptor_) {
        acceptor_.reset();
    }

    for (auto& item : connections_) {
        std::shared_ptr<Connection> conn = item.second;
        if (conn) {
            conn->SetCloseCallback(nullptr);
            EventLoop* io_loop = conn->GetLoop();
            io_loop->QueueInLoop([conn]() {
                conn->ConnectionDestroyed();
            });
        }
    }
    connections_.clear();
}