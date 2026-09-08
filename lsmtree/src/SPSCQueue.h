#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <unistd.h>
#include <sys/eventfd.h>
#include <poll.h>
#include <immintrin.h>
#include <cassert>

template <typename T, size_t Capacity = 65536>
class SPSCQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");

public:
    SPSCQueue() 
        : buffer_(Capacity),
          wakeup_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
        assert(wakeup_fd_ >= 0);
    }

    ~SPSCQueue() {
        if (wakeup_fd_ >= 0) {
            ::close(wakeup_fd_);
            wakeup_fd_ = -1;
        }
    }

    SPSCQueue(const SPSCQueue&) = delete;
    SPSCQueue& operator=(const SPSCQueue&) = delete;

    // 生产者调用：非阻塞入队
    bool Push(T&& item) {
        const size_t current_tail = tail_.load(std::memory_order_relaxed);
        
        // 当本地缓存判定满时，才同步一次消费者的真实进度
        if (current_tail - head_cached_ == Capacity) {
            head_cached_ = head_.load(std::memory_order_acquire);
            if (current_tail - head_cached_ == Capacity) {
                return false; // 队列已满（触发背压）
            }
        }

        buffer_[current_tail & (Capacity - 1)] = std::move(item);
        tail_.store(current_tail + 1, std::memory_order_release);

        // 如果消费者处于挂起状态，通过 eventfd 唤醒
        if (consumer_sleeping_.load(std::memory_order_seq_cst)) {
            Wakeup();
        }
        return true;
    }

    // 消费者调用：非阻塞出队
    bool Pop(T& item) {
        const size_t current_head = head_.load(std::memory_order_relaxed);

        // 当本地缓存判定空时，才同步一次生产者的真实进度
        if (current_head == tail_cached_) {
            tail_cached_ = tail_.load(std::memory_order_acquire);
            if (current_head == tail_cached_) {
                return false; // 队列为空
            }
        }

        item = std::move(buffer_[current_head & (Capacity - 1)]);
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    // 消费者调用：两段式自适应等待出队（自旋 -> 挂起）
    bool PopWait(T& item, const std::atomic<bool>& running) {
        while (true) {
            // 阶段 1：快速出队探测
            if (Pop(item)) {
                return true;
            }

            // 若停止信号已下发且队列已排空，安全退出
            if (!running.load(std::memory_order_acquire)) {
                return Pop(item);
            }

            // 阶段 2：自旋 64 次优化流水线（打流场景下在微秒级内即刻捕获新任务）
            for (int i = 0; i < 64; ++i) {
                _mm_pause();
                if (Pop(item)) {
                    return true;
                }
            }

            // 阶段 3：自旋未命中，标记准备睡眠并二次复核，防范竞态遗漏
            consumer_sleeping_.store(true, std::memory_order_seq_cst);
            if (Pop(item)) {
                consumer_sleeping_.store(false, std::memory_order_relaxed);
                return true;
            }
            if (!running.load(std::memory_order_acquire)) {
                consumer_sleeping_.store(false, std::memory_order_relaxed);
                return Pop(item);
            }

            // 阶段 4：休眠在 eventfd 上，带 50ms 超时防悬挂
            struct pollfd pfd;
            pfd.fd = wakeup_fd_;
            pfd.events = POLLIN;
            pfd.revents = 0;
            ::poll(&pfd, 1, 50);

            uint64_t val = 0;
            ::read(wakeup_fd_, &val, sizeof(val));
            consumer_sleeping_.store(false, std::memory_order_relaxed);
        }
    }

    void Wakeup() {
        uint64_t val = 1;
        ::write(wakeup_fd_, &val, sizeof(val));
    }

    size_t Size() const {
        size_t t = tail_.load(std::memory_order_relaxed);
        size_t h = head_.load(std::memory_order_relaxed);
        return (t >= h) ? (t - h) : 0;
    }

    bool Empty() const {
        return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_relaxed);
    }

private:
    // 生产者独占的缓存行
    alignas(64) std::atomic<size_t> tail_{0};
    alignas(64) size_t head_cached_{0};

    // 消费者独占的缓存行
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) size_t tail_cached_{0};

    // 跨线程唤醒同步标记
    alignas(64) std::atomic<bool> consumer_sleeping_{false};

    // 预分配大数组
    std::vector<T> buffer_;
    int wakeup_fd_{-1};
};