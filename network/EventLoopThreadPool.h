#pragma once

#include <vector>
#include <memory>
#include <cstddef>

class EventLoop;
class EventLoopThread;

class EventLoopThreadPool {
public:
    explicit EventLoopThreadPool(EventLoop* base_loop);
    ~EventLoopThreadPool();

    EventLoopThreadPool(const EventLoopThreadPool&) = delete;
    EventLoopThreadPool& operator=(const EventLoopThreadPool&) = delete;

    void SetThreadNum(int num_threads) { num_threads_ = num_threads; }
    void Start();

    // Round-Robin 负载均衡选择下一个 Sub-Reactor
    EventLoop* GetNextLoop();

    bool Started() const { return started_; }

    //供 TcpServer 遍历所有子 Reactor 并发挂载 TimingWheel
    const std::vector<EventLoop*>& GetAllLoops() const { return loops_; }

private:
    EventLoop* base_loop_; // 主 Reactor 实例
    bool started_{false};
    int num_threads_{0};
    size_t next_{0};
    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop*> loops_;
};