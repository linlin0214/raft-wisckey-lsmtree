#pragma once
#include "skiplist.h"
#include "ThreadWrite.h"
#include <string>
#include <cstdint>
#include <stdexcept>
#include <vector>
#include <cstring> 
#include <cstdio>   
#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <unistd.h> 

class WalManager {
public:
    //  构造函数新增 sync_mode 参数（默认不强刷盘，交由 Page Cache 或后台异步处理）
    WalManager(const std::string& log_path, bool sync_mode = false, bool disable = false) 
        : log_path_(log_path), sync_mode_(sync_mode), disable_(disable), stop_thread_(false), is_dirty_(false) {
        if (disable_) {
            return;
        }
        dest_ = std::fopen(log_path_.size() > 0 ? log_path_.c_str() : "default.wal", "ab"); 
        if (!dest_) {
            throw std::runtime_error("CAN'T OPEN LOG FILE: " + log_path_);
        }
        buffer_.reserve(kFlushThreshold);

        // 启动后台异步落盘线程
        bg_thread_ = std::thread(&WalManager::BackgroundFlush, this);
    }

    ~WalManager() {
        if (disable_) return;
        stop_thread_ = true;
        cv_.notify_all(); 
        
        if (bg_thread_.joinable()) {
            bg_thread_.join();
        }
        
        std::lock_guard<std::mutex> lock(mtx_);
        if (dest_) {
            SyncUnlocked(); 
            std::fclose(dest_);
            dest_ = nullptr;
        }
    }

    // 强一致性刷盘物理接口
    void Sync() {
        if (disable_) return;
        std::lock_guard<std::mutex> lock(mtx_);
        SyncUnlocked();
    }

    // 单条写入（仅进入内存，极速）
    void LogPut(int key, const std::string& value, int level) {
        if (disable_) return;
        char header[13];
        header[0] = 1; 
        std::memcpy(header + 1, &key, 4);
        std::memcpy(header + 5, &level, 4);
        
        uint32_t vlen = static_cast<uint32_t>(value.size());
        std::memcpy(header + 9, &vlen, 4);

        std::lock_guard<std::mutex> lock(mtx_);
        buffer_.append(header, 13);
        buffer_.append(value.data(), vlen);
        
        is_dirty_.store(true, std::memory_order_release); //  标记产生脏数据

        if (buffer_.size() >= kFlushThreshold) {
            SyncUnlocked(); 
        }
    }

    // 数据库崩溃冷启动恢复
    void recovery(skiplist& list) {
        if (disable_) return;
        std::lock_guard<std::mutex> lock(mtx_);
        if (dest_) std::fflush(dest_); 

        FILE* src = std::fopen(log_path_.c_str(), "rb");
        if (!src) return;

        std::fseek(src, 0, SEEK_END);
        long file_size = std::ftell(src);
        
        if (file_size > 0) {
            std::fseek(src, 0, SEEK_SET);
            std::vector<char> file_data(file_size);
            
            if (std::fread(file_data.data(), 1, file_size, src) == static_cast<size_t>(file_size)) {
                size_t offset = 0;
                while (offset + 7 <= static_cast<size_t>(file_size)) {
                    char type = file_data[offset];
                    if (type != 1) break; 
                    
                    uint16_t klen;
                    uint32_t vlen;
                    std::memcpy(&klen, file_data.data() + offset + 1, 2);
                    std::memcpy(&vlen, file_data.data() + offset + 3, 4);
                    
                    offset += 7;
                    if (offset + klen + vlen > static_cast<size_t>(file_size)) break; 
                    
                    std::string key(file_data.data() + offset, klen);
                    offset += klen;
                    std::string value(file_data.data() + offset, vlen);
                    offset += vlen;

                    list.insert(Slice(key), value, list.RandomLevel());
                }
            }
        }
        std::fclose(src);
    }

    void ResetLog() {
        if (disable_) return;
        std::lock_guard<std::mutex> lock(mtx_);
        buffer_.clear(); 
        is_dirty_.store(false, std::memory_order_release);
        if (dest_) std::fclose(dest_);
        dest_ = std::fopen(log_path_.c_str(), "wb"); 
        if (!dest_) throw std::runtime_error("ResetLog failed: " + log_path_);
    }

    // Group Commit 写入链
    void LogBatch(const ThreadWrite::WriteBatch& batch) {
        if (disable_) return;
        if (batch.entries.empty()) return;

        std::string batch_buffer;
        batch_buffer.reserve(batch.entries.size() * 128); 

        for (const auto& entry : batch.entries) {
            char header[7];
            header[0] = 1; // Record Type
            uint16_t klen = static_cast<uint16_t>(entry.key.size());
            uint32_t vlen = static_cast<uint32_t>(entry.value.size());

            std::memcpy(header + 1, &klen, 2);
            std::memcpy(header + 3, &vlen, 4);

            batch_buffer.append(header, 7);
            if (klen > 0) batch_buffer.append(entry.key.data(), klen);
            if (vlen > 0) batch_buffer.append(entry.value.data(), vlen);
        }

        std::lock_guard<std::mutex> lock(mtx_);
        if (dest_) {
            if (!buffer_.empty()) {
                std::fwrite(buffer_.data(), 1, buffer_.size(), dest_);
                buffer_.clear(); 
            }
            std::fwrite(batch_buffer.data(), 1, batch_buffer.size(), dest_);
            std::fflush(dest_);

            is_dirty_.store(true, std::memory_order_release);

            if (sync_mode_) {
                int fd = fileno(dest_);
                if (fd >= 0) {
                    ::fdatasync(fd); 
                    is_dirty_.store(false, std::memory_order_release);
                }
            }
        }
    }

private:
    void SyncUnlocked() {
        if (!dest_) return;
        if (!buffer_.empty()) {
            std::fwrite(buffer_.data(), 1, buffer_.size(), dest_);
            buffer_.clear();
        }
        std::fflush(dest_); 
        
        int fd = fileno(dest_);
        if (fd >= 0) {
            ::fdatasync(fd); 
        }
        is_dirty_.store(false, std::memory_order_release); // 刷完清空脏标记
    }

    // 异步刷盘守护线程优化版
    void BackgroundFlush() {
        while (!stop_thread_) {
            std::unique_lock<std::mutex> cv_lock(cv_mtx_);
            cv_.wait_for(cv_lock, std::chrono::milliseconds(100), [this] { return stop_thread_.load(); });
            
            if (stop_thread_) break;
            cv_lock.unlock(); 

            //  极致优化：先无锁检测脏标记。如果近期没有新写入，直接跳过，零开销、零抢锁摩擦！
            if (is_dirty_.load(std::memory_order_acquire)) {
                std::lock_guard<std::mutex> lock(mtx_);
                // 双重校验，防止抢锁期间前台已经主动 Sync 过了
                if (is_dirty_.load(std::memory_order_acquire)) {
                    SyncUnlocked();
                }
            }
        }
    }
    bool disable_{false};
    FILE* dest_ = nullptr; 
    std::string log_path_;
    std::string buffer_;
    static const size_t kFlushThreshold = 2 * 1024 * 1024; 

    std::mutex mtx_; 
    std::thread bg_thread_; 
    std::atomic<bool> stop_thread_;
    std::atomic<bool> is_dirty_; //  引入原子脏数据指针，规避 redundant fdatasync
    const bool sync_mode_;       //  控制是否强同步
    std::mutex cv_mtx_; 
    std::condition_variable cv_; 
};