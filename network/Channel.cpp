#include "Channel.h"
#include "EventLoop.h"
#include <cassert>

Channel::Channel(EventLoop* loop, int fd) 
    : loop_(loop), fd_(fd), events_(0), revents_(0) {}

Channel::~Channel() {
    //决不允许在派发事件回调的中途析构 Channel
    assert(!event_handling_);
}

void Channel::Tie(const std::shared_ptr<void>& obj) {
    tie_ = obj;
    tied_ = true;
}

void Channel::Update() {
    if (loop_) {
        loop_->UpdateChannel(this);
    }
}

void Channel::Remove() {
    if (loop_) {
        loop_->RemoveChannel(this);
    }
}

void Channel::EnableReading() { 
    events_ |= (EPOLLIN | EPOLLPRI | EPOLLRDHUP);
    Update();
}

void Channel::DisableReading() {
    events_ &= ~(EPOLLIN | EPOLLPRI | EPOLLRDHUP);
    Update();
}

void Channel::EnableWriting() { 
    events_ |= EPOLLOUT; 
    Update();
}

void Channel::DisableWriting() { 
    events_ &= ~EPOLLOUT; 
    Update();
}

void Channel::DisableAll() { 
    events_ = 0; 
    Update();
}

void Channel::HandleEvent() {
    if (tied_) {
        // 锁定上层 Connection 对象，在栈上持有强引用，防止回调中途对象被物理销毁
        std::shared_ptr<void> guard = tie_.lock();
        if (guard) {
            HandleEventWithGuard();
        }
    } else {
        HandleEventWithGuard();
    }
}

// 内部 RAII 守卫，刚性保障离开作用域时 event_handling_ 复位
struct EventHandlingGuard {
    bool& handling_;
    explicit EventHandlingGuard(bool& handling) : handling_(handling) {
        handling_ = true;
    }
    ~EventHandlingGuard() {
        handling_ = false;
    }
};

void Channel::HandleEventWithGuard() {
    EventHandlingGuard guard(event_handling_);

    // 1. 对端关闭连接 / 挂断事件 (且无待读数据)
    if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
        if (close_callback_) close_callback_();
    }

    // 2. 底层套接字报错
    if (revents_ & EPOLLERR) {
        if (error_callback_) error_callback_();
    }

    // 3. 读就绪事件 (EPOLLIN) 或 对端关闭 Half-Close (EPOLLRDHUP)
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) {
        if (read_callback_) read_callback_();
    }

    // 4. 写就绪事件 (EPOLLOUT)
    if (revents_ & EPOLLOUT) {
        if (write_callback_) write_callback_();
    }
}