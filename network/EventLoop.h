#pragma once

#include <memory>
#include <vector>
#include <functional>
#include <thread>
#include <mutex>
#include <atomic>
#include <string>
#include <unordered_map>


class Epoll;
class Channel;

class EventLoop {
public:
    EventLoop();
    ~EventLoop();

    // 禁用拷贝构造与赋值运算符，保证资源句柄独占性
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void Loop();
    void Quit();

    // 核心接口：若在 Loop 线程内直接执行，否则跨线程安全的异步投递
    void RunInLoop(std::function<void()> cb);

    // 线程安全的跨线程唤醒投递函数
    void QueueInLoop(std::function<void()> cb);

    void UpdateChannel(Channel* channel);
    void RemoveChannel(Channel* channel);

    void AssertInLoopThread();
    bool IsInLoopThread() const { return thread_id_ == std::this_thread::get_id(); }

    public:
    void SetContext(const std::string& key, std::shared_ptr<void> ctx) {
        contexts_[key] = std::move(ctx);
    }

    template <typename T>
    std::shared_ptr<T> GetContext(const std::string& key) const {
        auto it = contexts_.find(key);
        if (it != contexts_.end()) {
            return std::static_pointer_cast<T>(it->second);
        }
        return nullptr;
    }

private:
    void Wakeup();
    void HandleRead();
    void DoPendingFunctors();

private:
    using ChannelList = std::vector<Channel*>;

    std::unique_ptr<Epoll> epoll_;         // 独占的 Epoll 多路复用器
    std::atomic<bool> looping_{false};     // 标识是否正处于 Loop() 循环中
    std::atomic<bool> quit_{false};        // 停止事件循环标识
    bool calling_pending_functors_{false}; // 标识当前是否正在执行 pending_functors_ 回调 (仅宿主线程读写)

    std::thread::id thread_id_;            // 绑定的宿主线程 ID

    int wakeup_fd_;                        // Linux 原生 eventfd 句柄
    std::unique_ptr<Channel> wakeup_channel_; // 打理 wakeup_fd_ 读事件的专用 Channel

    std::mutex mutex_;                     // 保护 pending_functors_ 跨线程安全的互斥锁
    std::vector<std::function<void()>> pending_functors_; // 跨线程投递的待处理闭包任务池

    ChannelList active_channels_;          // 每次 epoll_wait 捞出的活跃通道数组

    std::unordered_map<std::string, std::shared_ptr<void>> contexts_;
};