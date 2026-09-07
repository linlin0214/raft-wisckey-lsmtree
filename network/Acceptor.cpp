#include "Acceptor.h"
#include "EventLoop.h"
#include "Channel.h"
#include <spdlog/spdlog.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <cstring>
#include <cerrno>
#include <cstdlib>

Acceptor::Acceptor(EventLoop* loop, const std::string& ip, uint16_t port)
    : loop_(loop), listen_fd_(-1), idle_fd_(::open("/dev/null", O_RDONLY | O_CLOEXEC)) {
    
    // 1. 创建 IPv4 ，非阻塞、防泄漏监听套接字
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) {
        spdlog::critical("[Acceptor] 创建监听套接字失败！errno: {}", errno);
        std::abort();
    }

    // 2. 开启地址复用（SO_REUSEADDR）与端口复用（SO_REUSEPORT）
    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

    // 3. 绑定 IP 与 端口契约
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);//转化端序
    ::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    if (::bind(listen_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        spdlog::critical("[Acceptor] 绑定端口 {} 失败！errno: {}", port, errno);
        std::abort();
    }

    // 4. 将 listen_fd 挂载至 EventLoop 控制面
    //在堆内存上创建一个 Channel 对象，并自动用一个 std::unique_ptr 智能指针把它包装起来返回
    accept_channel_ = std::make_unique<Channel>(loop_, listen_fd_);
    accept_channel_->SetReadCallback([this]() { this->HandleRead(); });
    
    spdlog::info("[Acceptor] 大门口基础设施架设完毕: {}:{}", ip, port);
}

Acceptor::~Acceptor() {
    if (accept_channel_) {
        accept_channel_->DisableAll();
        accept_channel_->Remove();
    }
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (idle_fd_ >= 0) {
        ::close(idle_fd_);
        idle_fd_ = -1;
    }
}

void Acceptor::Listen() {
    loop_->AssertInLoopThread();
    if (listening_) return;

    listening_ = true;

    // 1. 开启内核半连接与全连接队列监听
    if (::listen(listen_fd_, SOMAXCONN) < 0) {
        spdlog::critical("[Acceptor] Listen 动作被内核拒绝！errno: {}", errno);
        std::abort();
    }

    // 2. 挂载 Epoll 红黑树，开启 EPOLLIN 事件驱动
    accept_channel_->EnableReading();
    spdlog::info("[Acceptor] 物理套接字已开启 Listen 并正式挂载 Epoll 监听！");
}

void Acceptor::HandleRead() {
    loop_->AssertInLoopThread(); // 守卫单线程控制面

    //LT
    while (true) {
        struct sockaddr_in peer_addr;
        std::memset(&peer_addr, 0, sizeof(peer_addr));
        socklen_t addr_len = static_cast<socklen_t>(sizeof(peer_addr)); // 每次循环重置容量
        
        int client_fd = ::accept4(listen_fd_, (struct sockaddr*)&peer_addr, &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        
        if (client_fd >= 0) {
            char ip_str[INET_ADDRSTRLEN];
            ::inet_ntop(AF_INET, &peer_addr.sin_addr, ip_str, sizeof(ip_str));
            uint16_t port = ntohs(peer_addr.sin_port);
            spdlog::debug("[Acceptor] 成功捕获新连接！来自远端节点 -> {}:{}，分发 client_fd: {}", ip_str, port, client_fd);

            if (new_connection_callback_) {
                new_connection_callback_(client_fd);
            } else {
                // 未注册回调时立刻关闭 client_fd，绝不造成 fd 泄漏！
                spdlog::warn("[Acceptor] 捕获新连接 client_fd: {} 但未注册 new_connection_callback_，执行安全销毁！", client_fd);
                ::close(client_fd);
            }
        } else {
            // 1. 队列彻底捞空，平稳撤出
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            // 2. 软错误隔离：中途中断，继续尝试
            if (errno == ECONNABORTED || errno == EINTR) {
                continue;
            }
            // 3. 句柄耗尽防御 (EMFILE / ENFILE) - Muduo 紧急排空策略
            if (errno == EMFILE || errno == ENFILE) {
                spdlog::error("[Acceptor] 套接字描述符已耗尽 (EMFILE)！触发紧急熔断排空...");
                ::close(idle_fd_);
                idle_fd_ = ::accept(listen_fd_, nullptr, nullptr);
                if (idle_fd_ >= 0) {
                    ::close(idle_fd_);
                }
                idle_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
                break;
            }

            spdlog::error("[Acceptor] 发生未知的内核接收硬报错 errno: {}", errno);
            break;
        }
    }
}