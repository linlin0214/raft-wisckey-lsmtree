#pragma once
#include "VLogStruct.h"
#include "Slice.h"
#include <string>
#include <mutex>
#include <filesystem>
#include <cstdio>
#include <stdexcept>
#include <iostream>
#include <algorithm>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>

namespace fs = std::filesystem;

class ValueLog {
public:
    ValueLog(const std::string& log_dir, size_t max_file_size = 512 * 1024 * 1024) 
        : log_dir_(log_dir), max_file_size_(max_file_size), current_offset_(0) {
        
        fs::create_directories(log_dir_);
        
        uint32_t max_id = 1;
        for (const auto& entry : fs::directory_iterator(log_dir_)) {
            std::string fname = entry.path().filename().string();
            if (fname.length() >= 9 && 
                fname.compare(0, 5, "vlog_") == 0 && 
                fname.compare(fname.length() - 4, 4, ".log") == 0) {
                
                uint32_t id;
                if (sscanf(fname.c_str(), "vlog_%u.log", &id) == 1) {
                    max_id = std::max(max_id, id);
                }
            }
        }
        current_file_id_ = max_id;
        current_filename_ = log_dir_ + "/vlog_" + std::to_string(current_file_id_) + ".log";
        out_ = std::fopen(current_filename_.c_str(), "ab");
        if (!out_) throw std::runtime_error("vLog 写文件描述符分配失败！");
        
        std::fseek(out_, 0, SEEK_END);
        current_offset_ = std::ftell(out_);
        
        std::cout << "[ValueLog] 挂载成功，当前活跃写入段: vlog_" << current_file_id_ 
                  << " | 初始物理偏移: " << current_offset_ << " 字节" << std::endl;
    }

    ~ValueLog() {
        if (out_) {
            std::fflush(out_);
            int fd = fileno(out_);
            if (fd >= 0) { ::fdatasync(fd); }
            std::fclose(out_);
        }
    }

    void Sync() {
        std::lock_guard<std::mutex> lock(mtx_);
        if (out_) {
            std::fflush(out_); 
            int fd = fileno(out_);
            if (fd >= 0) {
                ::fdatasync(fd);
            }
        }
    }

    VLogPointer Append(const Slice& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(mtx_); 

        uint16_t key_len = static_cast<uint16_t>(key.size());
        uint32_t val_size = static_cast<uint32_t>(value.size());
        uint32_t write_bytes = sizeof(key_len) + sizeof(val_size) + key_len + val_size;

        if (current_offset_ + write_bytes > max_file_size_) {
            std::fflush(out_); 
            int fd = ::fileno(out_);
            if (fd >= 0) { ::fdatasync(fd); }
            std::fclose(out_); 
            
            current_file_id_++; 
            current_filename_ = log_dir_ + "/vlog_" + std::to_string(current_file_id_) + ".log";
            out_ = std::fopen(current_filename_.c_str(), "ab");
            if (!out_) throw std::runtime_error("vLog 新段文件描述符分配失败！");
            
            current_offset_ = 0; 
            std::cout << "[ValueLog] 触发段轮转，创建新活跃段: vlog_" << current_file_id_ << std::endl;
        }

        uint64_t val_offset = current_offset_ + sizeof(key_len) + sizeof(val_size) + key_len;

        if (std::fwrite(&key_len, 1, sizeof(key_len), out_) != sizeof(key_len) ||
            std::fwrite(&val_size, 1, sizeof(val_size), out_) != sizeof(val_size) ||
            (key_len > 0 && std::fwrite(key.data(), 1, key_len, out_) != key_len) ||
            (val_size > 0 && std::fwrite(value.data(), 1, val_size, out_) != val_size)) {
            throw std::runtime_error("[ValueLog] 严重物理写入失败！磁盘空间可能已满或遭遇 I/O 故障！");
        }

        std::fflush(out_);
        current_offset_ += write_bytes;
        return {current_file_id_, val_offset, val_size};
    }

    std::string Read(const VLogPointer& ptr) {
        std::string path = GetPath(ptr.file_id);
        int target_fd = ::open(path.c_str(), O_RDONLY);
        if (target_fd < 0) return "NOT_FOUND";

        std::string value(ptr.size, '\0');
        ssize_t bytes_read = ::pread(target_fd, &value[0], ptr.size, ptr.offset);
        ::close(target_fd);

        if (bytes_read != static_cast<ssize_t>(ptr.size)) {
            return "NOT_FOUND"; 
        }
        return value;
    }

    uint32_t GetCurrentFileId() const { return current_file_id_; }

    uint32_t GetOldestFileId() const {
        uint32_t min_id = 0xFFFFFFFF; 
        for (const auto& entry : fs::directory_iterator(log_dir_)) {
            std::string fname = entry.path().filename().string();
            if (fname.length() >= 9 && 
                fname.compare(0, 5, "vlog_") == 0 && 
                fname.compare(fname.length() - 4, 4, ".log") == 0) {
                
                uint32_t id;
                if (sscanf(fname.c_str(), "vlog_%u.log", &id) == 1) {
                    min_id = std::min(min_id, id);
                }
            }
        }
        return min_id == 0xFFFFFFFF ? 0 : min_id;
    }

    std::string GetPath(uint32_t file_id) const {
        return log_dir_ + "/vlog_" + std::to_string(file_id) + ".log";
    }

    void RemoveSegment(uint32_t file_id) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (file_id == current_file_id_) return; 
        std::error_code ec;
        fs::remove(GetPath(file_id), ec);
    }

private:
    std::string log_dir_;
    size_t max_file_size_; 
    FILE* out_ = nullptr; 
    
    std::string current_filename_;
    uint32_t current_file_id_;
    uint64_t current_offset_;
    
    std::mutex mtx_; 
};