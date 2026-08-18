#pragma once
#include <memory>
#include <functional>
#include <string>

class EventLoop;
class Channel;

class Acceptor {
public:
    // 当捞出一个合法的客户端非阻塞 fd 后，递交给上层（TcpServer）的回调槽
    using NewConnectionCallback = std::function<void(int client_fd)>;

private:
    EventLoop* loop_;                         // 挂载的事件循环大管家
    int listen_fd_{-1};                       // 专门负责监听的套接字
    int idle_fd_{-1};                         // 占位句柄：用于防御句柄耗尽死循环
    std::unique_ptr<Channel> accept_channel_; // 负责打理 listen_fd 读事件的经纪人
    bool listening_{false};                   // 监听状态管控标识

    NewConnectionCallback new_connection_callback_;

    // EPOLLIN 触发时贪婪收割新连接
    void HandleRead();

public:
    Acceptor(EventLoop* loop, const std::string& ip, uint16_t port);
    ~Acceptor();

    // 禁用拷贝构造与赋值运算符
    Acceptor(const Acceptor&) = delete;
    Acceptor& operator=(const Acceptor&) = delete;

    void SetNewConnectionCallback(NewConnectionCallback cb) { new_connection_callback_ = std::move(cb); }

    bool Listening() const { return listening_; }
    void Listen(); // 供 TcpServer::Start() 显式唤醒
};