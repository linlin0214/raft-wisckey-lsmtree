#pragma once
#include <memory>
#include <functional>

class EventLoop;
class Channel;

class TimerQueue {
public:
    using TimerCallback = std::function<void()>;

private:
    EventLoop* loop_;                         // 所属 EventLoop 控制面指针
    int timer_fd_{-1};                        // Linux timerfd 物理句柄
    std::unique_ptr<Channel> timer_channel_;  // 托管 timer_fd_ 的 Channel 经纪人
    TimerCallback timer_cb_;                  // 超时触发的 C++ 闭包回调

    void HandleRead();

public:
    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    TimerQueue(const TimerQueue&) = delete;
    TimerQueue& operator=(const TimerQueue&) = delete;

    void SetTimerCallback(TimerCallback cb) { timer_cb_ = std::move(cb); }

    // 线程安全控制接口：支持跨线程安全的重置与关闭
    void Reset(int timeout_ms);
    void Stop();
};