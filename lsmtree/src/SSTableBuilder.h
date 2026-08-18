#pragma once
#include <fstream>
#include <vector>
#include <string>
#include <memory>  
#include <iostream>
#include <cstring> 
#include <fcntl.h>
#include <unistd.h>
#include "skiplist.h"
#include "BloomFilter.h"

struct IndexEntry {
    int key;
    uint32_t offset;
};

class SSTableBuilder {
public:
    SSTableBuilder() : current_offset_(0), min_key_(0), max_key_(0), first_key_(true), final_file_size_(0) {
        io_buf_.resize(1024 * 1024); 
    }

    void Start(const std::string& sst_path, int expected_entries = 10000) {
        if (out_.is_open()) {
            out_.close();
        }
        
        current_sst_path_ = sst_path; //  记录当前 SST 文件绝对/相对物理路径
        
        out_.rdbuf()->pubsetbuf(io_buf_.data(), io_buf_.size());
        out_.open(sst_path, std::ios::binary | std::ios::out | std::ios::trunc);
        if (!out_.is_open()) {
            throw std::runtime_error("无法创建/打开 SSTable 物理文件: " + sst_path);
        }

        //  状态彻底清理，防止跨文件元数据污染
        current_offset_ = 0;
        min_key_ = 0;
        max_key_ = 0;
        final_file_size_ = 0;
        first_key_ = true;
        
        index_table_.clear();
        index_table_.reserve(std::max(expected_entries, 1000)); 
        
        bf_ = std::make_unique<BloomFilter>(std::max(expected_entries, 100), 10);
    }

    void Add(int key, const std::string& value) {
        if (!out_.is_open()) return;

        if (first_key_) {
            min_key_ = key;
            first_key_ = false;
        }
        max_key_ = std::max(max_key_, key); // 保证绝对单调递增覆盖

        index_table_.push_back({key, current_offset_});
        bf_->AddInt(key);

        uint32_t v_len = static_cast<uint32_t>(value.size());
        
        char header[8];
        std::memcpy(header, &key, 4);
        std::memcpy(header + 4, &v_len, 4);
        
        out_.write(header, 8);
        out_.write(value.data(), v_len);

        current_offset_ += (8 + v_len);
    }

    size_t CurrentSize() const {
        return current_offset_;
    }

    void Finish() {
        if (!out_.is_open()) return;

        uint32_t index_start_pos = current_offset_;
        
        if (!index_table_.empty()) {
            size_t index_bytes = index_table_.size() * sizeof(IndexEntry);
            out_.write(reinterpret_cast<const char*>(index_table_.data()), index_bytes);
            current_offset_ += static_cast<uint32_t>(index_bytes);
        }
        
        uint32_t bf_start_pos = current_offset_;
        const auto& bf_data = bf_->GetData();
        if (!bf_data.empty()) {
            out_.write(reinterpret_cast<const char*>(bf_data.data()), bf_data.size());
            current_offset_ += static_cast<uint32_t>(bf_data.size());
        }

        // 写入 16 字节 Footer
        char footer[16];
        std::memcpy(footer, &index_start_pos, 4);
        std::memcpy(footer + 4, &bf_start_pos, 4);
        std::memcpy(footer + 8, &min_key_, 4);
        std::memcpy(footer + 12, &max_key_, 4);
        out_.write(footer, 16);

        current_offset_ += 16;
        final_file_size_ = current_offset_;

        out_.flush(); 
        out_.close();

        // 文件落盘关闭后，向内核下发 Clean & Evict 指令
        if (!current_sst_path_.empty()) {
            int fd = ::open(current_sst_path_.c_str(), O_RDONLY);
            if (fd >= 0) {
                ::fdatasync(fd); 
                ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
                ::close(fd);
            }
        }
        
        first_key_ = true; 
    }

    void Build(skiplist& list, const std::string& sst_path) {
        try {
            int count = 0;
            auto it = list.Begin();
            while (it.Valid()) {
                count++;
                it.Next();
            }

            Start(sst_path, count); 

            it = list.Begin();
            while (it.Valid()) {
                Add(it.key(), it.value());
                it.Next();
            }
            Finish();
        } catch (const std::exception& e) {
            if (out_.is_open()) out_.close(); 
            std::cerr << "[SSTableBuilder] 严重落盘故障: " << e.what() << std::endl;
            throw; 
        }
    }

    int GetMinKey() const { return min_key_; }
    int GetMaxKey() const { return max_key_; }
    size_t GetFileSize() const { return final_file_size_; }

private:
    std::ofstream out_;
    std::string current_sst_path_; 
    std::vector<char> io_buf_; 
    uint32_t current_offset_;
    std::vector<IndexEntry> index_table_;
    std::unique_ptr<BloomFilter> bf_;
    
    int min_key_;
    int max_key_;
    bool first_key_;
    size_t final_file_size_ = 0; 
};