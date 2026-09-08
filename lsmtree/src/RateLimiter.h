#pragma once

#include <chrono>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <cstdint>

class TokenBucketRateLimiter{
public:
    // bytes_per_sec: 允许通过的最大字节速率 (如 20 * 1024 * 1024 表示 20MB/s)
    explicit TokenBucketRateLimiter(int64_t bytes_per_sec)
        : rate_bytes_per_sec_(bytes_per_sec),
          max_tokens_(std::max<int64_t>(bytes_per_sec, 4 * 1024 * 1024)), // 允许最多 1 秒或 4MB 的突发配额
          tokens_(max_tokens_),
          last_refill_time_micros_(NowMicros()) {}

    ~TokenBucketRateLimiter() = default;

    TokenBucketRateLimiter(const TokenBucketRateLimiter&) = delete;
    TokenBucketRateLimiter& operator=(const TokenBucketRateLimiter&) = delete;

    // 申请特定字节数的写入配额；若配额耗尽，阻塞等待直至令牌补充
    void Request(size_t bytes){
        if(rate_bytes_per_sec_ <=0 || bytes==0){
            return ;// <= 0 表示不限速
        }
        int64_t needed_bytes = static_cast<int64_t>(bytes);
        std::unique_lock<std::mutex> lock(mtx_);

        while (needed_bytes > 0) {
            RefillUnlocked();

            // 当前令牌充足，直接扣减并放行
            if (tokens_ >= needed_bytes) {
                tokens_ -= needed_bytes;
                break;
            }

            // 令牌不足，全额消费现有令牌，并计算剩余部分所需的睡眠微秒数
            needed_bytes -= tokens_;
            tokens_ = 0;

            int64_t sleep_micros = (needed_bytes * 1000000LL) / rate_bytes_per_sec_;
            // 每次最多休眠 50ms，防止系统停机时长时间卡死
            int64_t chunk_sleep = std::min<int64_t>(sleep_micros, 50000LL);
            
            cv_.wait_for(lock, std::chrono::microseconds(chunk_sleep));
        }
    }

    void SetRate(int64_t bytes_per_sec) {
        std::lock_guard<std::mutex> lock(mtx_);
        rate_bytes_per_sec_ = bytes_per_sec;
        max_tokens_ = std::max<int64_t>(bytes_per_sec, 4 * 1024 * 1024);
        RefillUnlocked();
    }

private:
    static int64_t NowMicros() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    void RefillUnlocked() {
        int64_t now = NowMicros();
        int64_t elapsed_micros = now - last_refill_time_micros_;
        if (elapsed_micros > 0) {
            int64_t new_tokens = (elapsed_micros * rate_bytes_per_sec_) / 1000000LL;
            if (new_tokens > 0) {
                tokens_ = std::min(max_tokens_, tokens_ + new_tokens);
                last_refill_time_micros_ = now;
            }
        }
    }

private:
    int64_t rate_bytes_per_sec_;
    int64_t max_tokens_;
    int64_t tokens_;
    int64_t last_refill_time_micros_;
    std::mutex mtx_;
    std::condition_variable cv_;
};