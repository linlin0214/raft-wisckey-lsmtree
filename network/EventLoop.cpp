#include "EventLoop.h"
#include "Epoll.h"
#include "Channel.h"
#include <spdlog/spdlog.h>
#include <cassert>
#include <sys/eventfd.h>
#include <unistd.h>
#include <cstdlib>

namespace {
int CreateEventfd() {
    int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (evtfd < 0) {
        spdlog::critical("[EventLoop] 创建 eventfd 失败！errno: {}", errno);
        std::abort();
    }
    return evtfd;
}
} // namespace

EventLoop::EventLoop()
    : epoll_(std::make_unique<Epoll>()),
      thread_id_(std::this_thread::get_id()),
      wakeup_fd_(CreateEventfd()),
      wakeup_channel_(std::make_unique<Channel>(this, wakeup_fd_)) {
    
    spdlog::info("[EventLoop] 初始化成功。初始创建线程 ID: {}, wakeup_fd: {}", 
                 std::hash<std::thread::id>{}(thread_id_), wakeup_fd_);

    //如果可读，就执行 HandleRead
    wakeup_channel_->SetReadCallback([this]() { this->HandleRead(); });
    wakeup_channel_->EnableReading(); 
}

EventLoop::~EventLoop() {
    wakeup_channel_->DisableAll();
    wakeup_channel_->Remove();
    ::close(wakeup_fd_);
    active_channels_.clear();
}

void EventLoop::Loop() {
    assert(!looping_);
    
    thread_id_ = std::this_thread::get_id();
    AssertInLoopThread(); 
    
    looping_ = true;
    quit_ = false;

    spdlog::info("[EventLoop] 核心引擎正式点火 (宿主线程 ID: {})，全面接管 Linux 内核事件流...",
                 std::hash<std::thread::id>{}(thread_id_));

    while (!quit_) {
        active_channels_.clear();

        // 直到内核发现 wakeup_fd_ 有数据了，Poll 才会返回
        epoll_->Poll(&active_channels_, 10000);

        // Poll 返回时，已经把 wakeup_channel_ 塞进了 active_channels_ 里
        for (Channel* channel : active_channels_) {
            channel->HandleEvent();
        }

        DoPendingFunctors();
    }

    spdlog::info("[EventLoop] 核心引擎平稳熄火，释放控制平面。");
    looping_ = false;
}

void EventLoop::RunInLoop(std::function<void()> cb) {
    if (IsInLoopThread()) {
        cb();
    } else {
        QueueInLoop(std::move(cb));
    }
}

void EventLoop::QueueInLoop(std::function<void()> cb) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_functors_.push_back(std::move(cb));
    }

    if (!IsInLoopThread() || calling_pending_functors_) {
        Wakeup();
    }
}

void EventLoop::Wakeup() {
    uint64_t one = 1;
    // 往 wakeup_fd_ 写入 8 个字节的数据
    ssize_t n = ::write(wakeup_fd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        spdlog::error("[EventLoop] Wakeup() 写入 eventfd 字节数异常，期望 8，实际 {}", n);
    }
}

void EventLoop::HandleRead() {
    uint64_t one = 1;
    //读操作清空缓冲区
    ssize_t n = ::read(wakeup_fd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        spdlog::error("[EventLoop] HandleRead() 读取 eventfd 字节数异常，期望 8，实际 {}", n);
    }
}

void EventLoop::DoPendingFunctors() {
    std::vector<std::function<void()>> functors;
    calling_pending_functors_ = true;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        functors.swap(pending_functors_);
    }

    for (const auto& functor : functors) {
        functor(); 
    }

    calling_pending_functors_ = false;
}

void EventLoop::Quit() {
    quit_ = true;
    if (!IsInLoopThread()) {
        Wakeup();
    }
}

void EventLoop::UpdateChannel(Channel* channel) {
    AssertInLoopThread(); 
    epoll_->UpdateChannel(channel);
}

void EventLoop::RemoveChannel(Channel* channel) {
    AssertInLoopThread();
    epoll_->RemoveChannel(channel);
}

void EventLoop::AssertInLoopThread() {
    if (!IsInLoopThread()) {
        spdlog::critical("[EventLoop] 遭遇致命越界调用！试图跨越线程边界操控红黑树控制面！宿主线程: {}, 越界线程: {}", 
                         std::hash<std::thread::id>{}(thread_id_), 
                         std::hash<std::thread::id>{}(std::this_thread::get_id()));
        std::abort(); 
    }
}