#pragma once

#include <functional>
#include <cstdint>
#include <memory>
#include <sys/epoll.h>

class EventLoop;

class Channel {
public:
    //  定义一个类型别名，代表“没有参数、没有返回值的可调用对象”，实则是eventcallbacktype
    using EventCallback = std::function<void()>;

    // Epoll 红黑树挂载状态
    enum class Status : int {
        kNew = -1,     // 从未添加到 epoll
        kAdded = 1,    // 已添加到 epoll
        kDeleted = 2   // 已从 epoll 卸载
    };

private:
    EventLoop* loop_;       // 所属 EventLoop 句柄，用于向 Epoll 注册/卸载
    const int   fd_;         // 托管的文件描述符 (const 锁定，生命周期内不可变)
    uint32_t   events_;     // 用户关心的事件位掩码 (如 EPOLLIN | EPOLLOUT)
    uint32_t   revents_;    // 内核 epoll_wait 实际返回并触发的事件位掩码
    Status     status_{Status::kNew}; // 在 Epoll 红黑树上的挂载状态 (kNew / kAdded / kDeleted)

    std::weak_ptr<void> tie_; // 绑定的上层对象 weak_ptr 存根
    bool tied_{false};        // 是否开启了生命周期绑定标识
    bool event_handling_{false}; // 标识当前是否正在执行 HandleEvent 回调

    EventCallback read_callback_;  // 读事件就绪回调 (EPOLLIN / EPOLLPRI / EPOLLRDHUP)，eventcallbacktype
    EventCallback write_callback_; // 写事件就绪回调 (EPOLLOUT)
    EventCallback error_callback_; // 套接字报错回调 (EPOLLERR)
    EventCallback close_callback_; // 连接断开/挂断回调 (EPOLLHUP)

public:
    Channel(EventLoop* loop, int fd);
    ~Channel();

    // 禁用拷贝构造与赋值，保证 Channel 的唯一性
    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    int      GetFd() const { return fd_; }
    uint32_t GetEvents() const { return events_; }
    void     SetRevents(uint32_t ev) { revents_ = ev; }
    
    Status   GetStatus() const { return status_; }
    void     SetStatus(Status status) { status_ = status; }

    // 把 Channel 与上层对象 (如 Connection) 绑定生命期
    void Tie(const std::shared_ptr<void>& obj);

    // 操纵事件位运算
    void EnableReading();
    void DisableReading();
    void EnableWriting();
    void DisableWriting();
    void DisableAll();

    bool IsWriting() const { return events_ & EPOLLOUT; }
    bool IsReading() const { return events_ & (EPOLLIN | EPOLLPRI); }

    // 注册业务层回调
    void SetReadCallback(EventCallback cb)   { read_callback_ = std::move(cb); }
    void SetWriteCallback(EventCallback cb)  { write_callback_ = std::move(cb); }
    void SetErrorCallback(EventCallback cb)  { error_callback_ = std::move(cb); }
    void SetCloseCallback(EventCallback cb)  { close_callback_ = std::move(cb); }

    // 事件派发核心入口
    void HandleEvent();
    void Remove();

private:
    void Update();
    void HandleEventWithGuard();
};