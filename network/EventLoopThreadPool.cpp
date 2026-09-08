#include "EventLoopThreadPool.h"
#include "EventLoopThread.h"
#include "EventLoop.h"

EventLoopThreadPool::EventLoopThreadPool(EventLoop* base_loop)
    : base_loop_(base_loop) {}

EventLoopThreadPool::~EventLoopThreadPool() = default;

void EventLoopThreadPool::Start() {
    base_loop_->AssertInLoopThread();
    started_ = true;

    for (int i = 0; i < num_threads_; ++i) {
        auto t = std::make_unique<EventLoopThread>();
        loops_.push_back(t->StartLoop());
        threads_.push_back(std::move(t));
    }
}

EventLoop* EventLoopThreadPool::GetNextLoop() {
    base_loop_->AssertInLoopThread();
    EventLoop* loop = base_loop_;

    // 若配置了从属线程池，按轮询派发；未配置则退化为单 Reactor
    if (!loops_.empty()) {
        loop = loops_[next_];
        next_ = (next_ + 1) % loops_.size();
    }
    return loop;
}