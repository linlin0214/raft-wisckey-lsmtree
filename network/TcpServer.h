#pragma once
#include "Connection.h"
#include <memory>
#include <unordered_map>
#include <functional>
#include <string>
#include <atomic>

class EventLoop;
class Acceptor;

class TcpServer {
public:
    using ConnectionCallback = std::function<void(const std::shared_ptr<Connection>&)>;
    using MessageCallback = std::function<void(const std::shared_ptr<Connection>&, Buffer*)>;

private:
    EventLoop* loop_;                                                   // 核心 EventLoop 指针
    std::unique_ptr<Acceptor> acceptor_;                               // 监听迎宾经理
    std::unordered_map<int, std::shared_ptr<Connection>> connections_;  // 全场连接控制 Map

    ConnectionCallback connection_callback_;
    MessageCallback message_callback_;

    std::atomic<bool> started_{false};                                 // 原子启动控制标识

    // 内部核心事件路由
    void NewConnection(int sockfd);
    void RemoveConnection(const std::shared_ptr<Connection>& conn);
    void RemoveConnectionInLoop(const std::shared_ptr<Connection>& conn);

public:
    TcpServer(EventLoop* loop, const std::string& ip, uint16_t port);
    ~TcpServer();

    // 严禁拷贝
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void SetConnectionCallback(ConnectionCallback cb) { connection_callback_ = std::move(cb); }
    void SetMessageCallback(MessageCallback cb) { message_callback_ = std::move(cb); }

    void Start();
};