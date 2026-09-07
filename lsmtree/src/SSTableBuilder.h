#pragma once

#include "skiplist.h"
#include "BloomFilter.h"
#include "Slice.h"
#include <fstream>
#include <vector>
#include <string>
#include <memory>  
#include <cstring> 
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>

struct IndexEntry {
    std::string key;
    uint32_t offset;
};

class SSTableBuilder {
public:
    SSTableBuilder() : current_offset_(0), first_key_(true), final_file_size_(0) {
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
        min_key_.clear();
        max_key_.clear();
        final_file_size_ = 0;
        first_key_ = true;
        
        index_table_.clear();
        index_table_.reserve(std::max(expected_entries, 1000)); 
        bf_ = std::make_unique<BloomFilter>(std::max(expected_entries, 100), 10);
    }

    void Add(const Slice& key, const std::string& value) {
        if (!out_.is_open()) return;

        if (first_key_) {
            min_key_ = key.ToString();
            first_key_ = false;
        }
        max_key_ = key.ToString();

        index_table_.push_back({key.ToString(), current_offset_});
        bf_->Add(key);

        uint16_t k_len = static_cast<uint16_t>(key.size());
        uint32_t v_len = static_cast<uint32_t>(value.size());
        
        char header[6];
        std::memcpy(header, &k_len, 2);
        std::memcpy(header + 2, &v_len, 4);
        
        out_.write(header, 6);
        if (k_len > 0) out_.write(key.data(), k_len);
        if (v_len > 0) out_.write(value.data(), v_len);

        current_offset_ += (6 + k_len + v_len);
    }

    size_t CurrentSize() const { return current_offset_; }

    void Finish() {
        if (!out_.is_open()) return;

        uint32_t index_start_pos = current_offset_;
        
        // 1. 序列化 Index Block
        uint32_t entry_count = static_cast<uint32_t>(index_table_.size());
        out_.write(reinterpret_cast<const char*>(&entry_count), 4);
        current_offset_ += 4;

        uint16_t min_len = static_cast<uint16_t>(min_key_.size());
        out_.write(reinterpret_cast<const char*>(&min_len), 2);
        if (min_len > 0) out_.write(min_key_.data(), min_len);
        current_offset_ += (2 + min_len);

        uint16_t max_len = static_cast<uint16_t>(max_key_.size());
        out_.write(reinterpret_cast<const char*>(&max_len), 2);
        if (max_len > 0) out_.write(max_key_.data(), max_len);
        current_offset_ += (2 + max_len);

        for (const auto& entry : index_table_) {
            uint16_t klen = static_cast<uint16_t>(entry.key.size());
            out_.write(reinterpret_cast<const char*>(&klen), 2);
            if (klen > 0) out_.write(entry.key.data(), klen);
            out_.write(reinterpret_cast<const char*>(&entry.offset), 4);
            current_offset_ += (2 + klen + 4);
        }
        uint32_t index_bytes = current_offset_ - index_start_pos;

        // 2. 序列化 BloomFilter Block
        uint32_t bf_start_pos = current_offset_;
        const auto& bf_data = bf_->GetData();
        if (!bf_data.empty()) {
            out_.write(reinterpret_cast<const char*>(bf_data.data()), bf_data.size());
            current_offset_ += static_cast<uint32_t>(bf_data.size());
        }
        uint32_t bf_bytes = current_offset_ - bf_start_pos;

        // 3. 写入末尾 16 字节定长 Footer
        char footer[16];
        std::memcpy(footer, &index_start_pos, 4);
        std::memcpy(footer + 4, &index_bytes, 4);
        std::memcpy(footer + 8, &bf_start_pos, 4);
        std::memcpy(footer + 12, &bf_bytes, 4);
        out_.write(footer, 16);

        current_offset_ += 16;
        final_file_size_ = current_offset_;

        out_.flush(); 
        out_.close();

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

    void Build(const skiplist& list, const std::string& sst_path) {
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
    }

    std::string GetMinKey() const { return min_key_; }
    std::string GetMaxKey() const { return max_key_; }
    size_t GetFileSize() const { return final_file_size_; }

private:
    std::ofstream out_;
    std::string current_sst_path_; 
    std::vector<char> io_buf_; 
    uint32_t current_offset_;
    std::vector<IndexEntry> index_table_;
    std::unique_ptr<BloomFilter> bf_;
    
    std::string min_key_;
    std::string max_key_;
    bool first_key_;
    size_t final_file_size_{0}; 
};