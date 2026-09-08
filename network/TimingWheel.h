#pragma once

#include "EventLoop.h"
#include "TimerQueue.h"
#include "Connection.h"
#include <vector>
#include <unordered_set>
#include <memory>
#include <spdlog/spdlog.h>

class TimingWheel {
public:
    // RAII 连接生命期守卫
    struct Entry {
        explicit Entry(const std::weak_ptr<Connection>& conn) : weak_conn_(conn) {}
        
        ~Entry() {
            auto conn = weak_conn_.lock();
            if (conn && conn->IsConnected()) {
                spdlog::warn("[TimingWheel] 检测到客户端空闲超时超过阈值，强制断开僵尸连接 fd: {}", conn->GetFd());
                conn->ForceClose();
            }
        }
        std::weak_ptr<Connection> weak_conn_;
    };

    using EntryPtr = std::shared_ptr<Entry>;
    using Bucket = std::unordered_set<EntryPtr>;

    TimingWheel(EventLoop* loop, int idle_seconds = 60)
        : loop_(loop),
          idle_seconds_(idle_seconds > 0 ? idle_seconds : 60),
          wheel_(idle_seconds_),
          head_(0),
          timer_(std::make_unique<TimerQueue>(loop)) {
        timer_->SetTimerCallback([this]() { OnTick(); });
        timer_->Reset(1000); // 刚性 1 秒心跳脉冲
    }

    ~TimingWheel() {
        if (timer_) {
            timer_->Stop();
        }
        wheel_.clear();
    }

    TimingWheel(const TimingWheel&) = delete;
    TimingWheel& operator=(const TimingWheel&) = delete;

    // 新连接首次注册接入
    void Register(const std::shared_ptr<Connection>& conn) {
        auto entry = std::make_shared<Entry>(conn);
        conn->SetCustomContext(entry);
        Touch(conn);
    }

    // 活跃数据刷新：将引用更新至最新桶内，延展 60 秒寿命
    void Touch(const std::shared_ptr<Connection>& conn) {
        auto entry = conn->GetCustomContext<Entry>();
        if (entry) {
            size_t tail = (head_ + idle_seconds_ - 1) % idle_seconds_;
            wheel_[tail].insert(entry);
        }
    }

private:
    void OnTick() {
        loop_->AssertInLoopThread();
        // 弹出并释放最老一格的全部 Entry 引用
        wheel_[head_].clear();
        head_ = (head_ + 1) % idle_seconds_;
        timer_->Reset(1000);
    }

private:
    EventLoop* loop_;
    const size_t idle_seconds_;
    std::vector<Bucket> wheel_;
    size_t head_{0};
    std::unique_ptr<TimerQueue> timer_;
};