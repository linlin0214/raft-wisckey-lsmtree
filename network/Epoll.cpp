#include "Epoll.h"
#include "Channel.h"
#include <spdlog/spdlog.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>

Epoll::Epoll() : events_(kInitEventListSize) {
    // 创建一个 epoll 实例（本质上是在内核里建了一棵红黑树），EPOLL_CLOEXEC 防止子进程继承这个 fd
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        spdlog::critical("[Epoll] 内核 epoll 实例创建失败! errno: {}", errno);
        std::abort();
    }
}

Epoll::~Epoll() {
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
    }
}
//负责把用户的 Channel（关心什么事件）同步到内核的 epoll 红黑树上
void Epoll::UpdateChannel(Channel* channel) {
    //获取状态
    const Channel::Status status = channel->GetStatus();
    const int fd = channel->GetFd();

    epoll_event ev;
    std::memset(&ev, 0, sizeof(ev));
    ev.events = channel->GetEvents();
    ev.data.ptr = channel; //把 Channel 指针塞进 epoll_event 里，不需要再去用 fd 查表了，O(1) 复杂度

    // 1. 未挂载 (kNew) 或 已卸载 (kDeleted)：执行 EPOLL_CTL_ADD
    if (status == Channel::Status::kNew || status == Channel::Status::kDeleted) {
        if (ev.events > 0) { // 仅当有关心事件时才挂载
            if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
                spdlog::error("[Epoll] 红黑树挂载失败 ADD fd: {}, errno: {}", fd, errno);
            } else {
                channel->SetStatus(Channel::Status::kAdded);
            }
        }
    } 
    // 2. 已挂载 (kAdded)：根据关心的事件掩码执行 MOD 或 DEL
    else if (status == Channel::Status::kAdded) {
        if (ev.events == 0) {
            // 没有任何关心的事件时，自动从内核红黑树卸载，减轻内核红黑树的负担
            if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
                spdlog::error("[Epoll] 自动卸载失败 DEL fd: {}, errno: {}", fd, errno);
            }
            channel->SetStatus(Channel::Status::kDeleted);
        } else {
            if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
                spdlog::error("[Epoll] 红黑树修改事件失败 MOD fd: {}, errno: {}", fd, errno);
            }
        }
    }
}

//将事件从红黑树卸载，访问一个已经关闭的fd
void Epoll::RemoveChannel(Channel* channel) {
    const int fd = channel->GetFd();
    const Channel::Status status = channel->GetStatus();

    if (status == Channel::Status::kAdded) {
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
            spdlog::error("[Epoll] 从红黑树卸载通道失败 DEL fd: {}, errno: {}", fd, errno);
        }
        channel->SetStatus(Channel::Status::kDeleted);
    }
}

//EventLoop线程无限循环调用的函数
void Epoll::Poll(std::vector<Channel*>* active_channels, int timeout_ms) {
    // 阻塞等待，直到有事件发生，或者超时
    int num_events = ::epoll_wait(epoll_fd_, events_.data(), static_cast<int>(events_.size()), timeout_ms);
    int saved_errno = errno;

    if (num_events > 0) {
        // 遍历所有触发的事件
        for (int i = 0; i < num_events; ++i) {
            // 从 epoll_event 里把当初塞进去的 Channel 指针取出
            Channel* channel = static_cast<Channel*>(events_[i].data.ptr);
            // 把内核返回的实际发生的事件（如 EPOLLIN | EPOLLOUT）存进 Channel
            channel->SetRevents(events_[i].events);
            // 把这个活跃的 Channel 放进输出列表，交给上层 EventLoop 去处理
            active_channels->push_back(channel);
        }

        // 动态扩容
        if (static_cast<size_t>(num_events) == events_.size() && events_.size() < kMaxEventListSize) {
            events_.resize(std::min(events_.size() * 2, static_cast<size_t>(kMaxEventListSize)));
        }
    } else if (num_events < 0) {
        if (saved_errno != EINTR) {
            spdlog::error("[Epoll] 内核轮询报错 epoll_wait errno: {}", saved_errno);
        }
    }
}