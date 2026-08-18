#include "TimerQueue.h"
#include "EventLoop.h"
#include "Channel.h"
#include <sys/timerfd.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <spdlog/spdlog.h>
#include <cstdlib>

TimerQueue::TimerQueue(EventLoop* loop) : loop_(loop), timer_fd_(-1) {
    timer_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ < 0) {
        spdlog::critical("[TimerQueue] 创建 Linux timerfd 失败！errno: {}", errno);
        std::abort();
    }

    timer_channel_ = std::make_unique<Channel>(loop_, timer_fd_);
    timer_channel_->SetReadCallback([this]() { this->HandleRead(); });
    timer_channel_->EnableReading();
}

TimerQueue::~TimerQueue() {
    loop_->AssertInLoopThread();
    
    //析构时先向内核注入全 0 itimerspec 清理/注销定时器 (Disarm)，防止悬空事件触发
    struct itimerspec ts{};
    std::memset(&ts, 0, sizeof(ts));
    ::timerfd_settime(timer_fd_, 0, &ts, nullptr);

    timer_channel_->DisableAll();
    timer_channel_->Remove();
    ::close(timer_fd_);
}

void TimerQueue::Reset(int timeout_ms) {
    auto func = [this, timeout_ms]() {
        struct itimerspec new_value;
        std::memset(&new_value, 0, sizeof(new_value));

        if (timeout_ms <= 0) {
            new_value.it_value.tv_sec = 0;
            new_value.it_value.tv_nsec = 1; 
        } else {
            new_value.it_value.tv_sec = timeout_ms / 1000;
            new_value.it_value.tv_nsec = static_cast<long>((timeout_ms % 1000) * 1000000LL);
        }

        if (::timerfd_settime(timer_fd_, 0, &new_value, nullptr) < 0) {
            spdlog::error("[TimerQueue] 调用 timerfd_settime 注入时间失败！errno: {}", errno);
        }
    };

    if (loop_->IsInLoopThread()) {
        func();
    } else {
        loop_->RunInLoop(func);
    }
}

void TimerQueue::Stop() {
    auto func = [this]() {
        struct itimerspec ts{};
        std::memset(&ts, 0, sizeof(ts));
        ::timerfd_settime(timer_fd_, 0, &ts, nullptr);
    };

    if (loop_->IsInLoopThread()) {
        func();
    } else {
        loop_->RunInLoop(func);
    }
}

void TimerQueue::HandleRead() {
    loop_->AssertInLoopThread();
    
    uint64_t expired = 0;
    ssize_t n = ::read(timer_fd_, &expired, sizeof(expired));
    
    if (n != sizeof(expired)) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        }
        spdlog::error("[TimerQueue] 消费内核定时器事件读取字节错位！errno: {}", errno);
        return;
    }

    if (timer_cb_) {
        timer_cb_();
    }
}