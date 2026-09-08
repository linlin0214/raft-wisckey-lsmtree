#pragma once

#include <mutex>
#include <condition_variable>
#include <thread>

class EventLoop;

class EventLoopThread {
public:
    EventLoopThread();
    ~EventLoopThread();

    EventLoopThread(const EventLoopThread&) = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;

    // 启动线程并在其内部实例化 EventLoop，阻塞等待直至 EventLoop 初始化完毕
    EventLoop* StartLoop();

private:
    void ThreadFunc();

private:
    EventLoop* loop_{nullptr};
    std::thread thread_;
    std::mutex mtx_;
    std::condition_variable cv_;
};