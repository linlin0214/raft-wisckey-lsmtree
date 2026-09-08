#pragma once
#include "Connection.h"
#include <memory>
#include <unordered_map>
#include <functional>
#include <string>
#include <atomic>

class EventLoop;
class Acceptor;
class EventLoopThreadPool;

class TcpServer {
public:
    using ConnectionCallback = std::function<void(const std::shared_ptr<Connection>&)>;
    using MessageCallback = std::function<void(const std::shared_ptr<Connection>&, Buffer*)>;

private:
    EventLoop* loop_;                                                   
    std::unique_ptr<Acceptor> acceptor_;                               
    std::unique_ptr<EventLoopThreadPool> thread_pool_; // Sub-Reactor 线程池
    std::unordered_map<int, std::shared_ptr<Connection>> connections_;  

    ConnectionCallback connection_callback_;
    MessageCallback message_callback_;

    std::atomic<bool> started_{false};                                 

    void NewConnection(int sockfd);
    void RemoveConnection(const std::shared_ptr<Connection>& conn);
    void RemoveConnectionInLoop(const std::shared_ptr<Connection>& conn);

public:
    TcpServer(EventLoop* loop, const std::string& ip, uint16_t port);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // 设置从属 Sub-Reactor 线程数量（0 表示单 Reactor，>0 启用多 Reactor）
    void SetThreadNum(int num_threads);

    void SetConnectionCallback(ConnectionCallback cb) { connection_callback_ = std::move(cb); }
    void SetMessageCallback(MessageCallback cb) { message_callback_ = std::move(cb); }

    void Start();
    void Stop();
};