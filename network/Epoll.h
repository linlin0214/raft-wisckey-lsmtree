#pragma once
#include <sys/epoll.h>
#include <vector>

class Channel;

class Epoll {
public:
    Epoll();
    ~Epoll();

    // 基础设施对象禁止拷贝与赋值
    Epoll(const Epoll&) = delete;
    Epoll& operator=(const Epoll&) = delete;

    // 控制内核红黑树的挂载、修改与卸载
    void UpdateChannel(Channel* channel);
    void RemoveChannel(Channel* channel);

    // 核心事件轮询
    void Poll(std::vector<Channel*>* active_channels, int timeout_ms = -1);

private:
    int epoll_fd_{-1};// 内核 epoll 实例句柄
    std::vector<epoll_event> events_;// 接收内核就绪事件的高速缓冲数组

    static constexpr int kInitEventListSize = 1024;// 默认初始可容纳 1024 个同时就绪事件
    static constexpr int kMaxEventListSize = 65536; // 限制事件池最大扩容上限(64K)
};