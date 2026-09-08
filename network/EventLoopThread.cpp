#include "EventLoopThread.h"
#include "EventLoop.h"

EventLoopThread::EventLoopThread() = default;

EventLoopThread::~EventLoopThread() {
    if (loop_) {
        loop_->Quit();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
}

EventLoop* EventLoopThread::StartLoop() {
    thread_ = std::thread(&EventLoopThread::ThreadFunc, this);

    EventLoop* loop = nullptr;
    {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait(lock, [this]() { return loop_ != nullptr; });
        loop = loop_;
    }
    return loop;
}

void EventLoopThread::ThreadFunc() {
    EventLoop loop;

    {
        std::lock_guard<std::mutex> lock(mtx_);
        loop_ = &loop;
        cv_.notify_one();
    }

    // 阻塞在子线程的 epoll_wait 驱动事件流
    loop.Loop();

    std::lock_guard<std::mutex> lock(mtx_);
    loop_ = nullptr;
}