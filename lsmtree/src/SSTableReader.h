#pragma once
#include <sys/types.h>
#include <vector>
#include <string>
#include <algorithm>
#include <cstdint>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include "SSTableBuilder.h"
#include "BloomFilter.h"
#include "LRUCache.h"

class SSTableReader {
public:
    SSTableReader(const std::string& filename) 
        : filename_(filename), bloom_filter_() {
        
        fd_ = ::open(filename.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw std::runtime_error("无法打开 SSTable 文件: " + filename);
        }
        LoadMetadata();
    }

    ~SSTableReader() {
        if (fd_ > 0) {
            ::close(fd_);
        }
    }

    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;
    SSTableReader(SSTableReader&&) = delete;
    SSTableReader& operator=(SSTableReader&&) = delete;

    bool MightContain(int key) const {
        return bloom_filter_.MightContainInt(key);
    }

    std::string Search(int key, ShardedLRUCache* cache = nullptr) {
        if (key < min_key_ || key > max_key_) return ""; 

        if (!MightContain(key)) return "";

        std::string cache_key;
        if (cache) {
            cache_key = filename_ + "_" + std::to_string(key);
            std::string cached_val;
            if (cache->Get(cache_key, cached_val)) {
                return cached_val; 
            }
        }

        auto it = std::lower_bound(index_.begin(), index_.end(), key, 
            [](const IndexEntry& a, int k) { return a.key < k; });

        if (it != index_.end() && it->key == key) {
            std::string disk_val = ReadValueAt(it->offset);
            
            if (cache && !disk_val.empty()) {
                cache->Put(cache_key, disk_val);
            }
            
            return disk_val;
        }
        return ""; 
    }

    int GetMinKey() const { return min_key_; }
    int GetMaxKey() const { return max_key_; }
    int GetLevel() const { return level_; }
    int GetBlockCount() const { return static_cast<int>(index_.size()); }
    std::string GetFilename() const { return filename_; }

    std::vector<std::pair<int, std::string>> LoadBlock(int start_index) {
        std::vector<std::pair<int, std::string>> res;
        if (start_index < 0 || start_index >= static_cast<int>(index_.size())) return res;

        int end_index = std::min(static_cast<int>(index_.size()), start_index + 256);
        res.reserve(end_index - start_index);

        for (int i = start_index; i < end_index; ++i) {
            uint32_t curr_offset = index_[i].offset;
            int key;
            uint32_t v_len;
            char header[8];
            
            if (::pread(fd_, header, 8, curr_offset) != 8) {
                continue; 
            }
            
            std::memcpy(&key, header, 4);
            std::memcpy(&v_len, header + 4, 4);

            if (v_len == 0 || v_len > 1024 * 1024) { 
                continue; 
            }

            std::string vlog_ptr_raw(v_len, '\0');
            if (::pread(fd_, &vlog_ptr_raw[0], v_len, curr_offset + 8) != static_cast<ssize_t>(v_len)) {
                continue;
            }
            
            res.push_back({key, std::move(vlog_ptr_raw)});
        }
        return res;
    }

private:
    std::string filename_;
    int fd_ = -1;     
    std::vector<IndexEntry> index_;
    
    BloomFilter bloom_filter_; 
    int min_key_ = 0;  
    int max_key_ = 0;  
    int level_ = 0;    

    void LoadMetadata() {
        off_t file_size = ::lseek(fd_, 0, SEEK_END);
        if (file_size == -1 || file_size < 16) return;

        char footer[16];
        if (::pread(fd_, footer, 16, file_size - 16) != 16) {
            return; 
        }
        
        uint32_t index_offset, bloom_offset;
        std::memcpy(&index_offset, footer, 4);
        std::memcpy(&bloom_offset, footer + 4, 4);
        std::memcpy(&min_key_, footer + 8, 4);
        std::memcpy(&max_key_, footer + 12, 4);

        if (bloom_offset <= index_offset || static_cast<uint32_t>(file_size) < bloom_offset) return;

        uint32_t index_size = bloom_offset - index_offset;
        int num_entries = index_size / sizeof(IndexEntry); 
        
        index_.resize(num_entries); 
        if (::pread(fd_, reinterpret_cast<char*>(index_.data()), index_size, index_offset) != static_cast<ssize_t>(index_size)) {
            return;
        }

        uint32_t bloom_size = static_cast<uint32_t>(file_size) - 16 - bloom_offset;
        std::vector<uint8_t> bf_data(bloom_size);
        if (::pread(fd_, reinterpret_cast<char*>(bf_data.data()), bloom_size, bloom_offset) != static_cast<ssize_t>(bloom_size)) {
            return;
        }

        bloom_filter_ = BloomFilter(bf_data); 
    }

    std::string ReadValueAt(uint32_t offset) {
        char header[8];
        if (::pread(fd_, header, 8, offset) != 8) return ""; 
        
        uint32_t v_len;
        std::memcpy(&v_len, header + 4, 4);

        if (v_len == 0 || v_len > 1024 * 1024) return ""; 
        std::string vlog_ptr_raw(v_len, '\0');
        if (::pread(fd_, &vlog_ptr_raw[0], v_len, offset + 8) != static_cast<ssize_t>(v_len)) {
            return "";
        }
        
        return vlog_ptr_raw; 
    }
};

class SSTableIterator {
public:
    SSTableIterator(SSTableReader* reader)
        : reader_(reader), curr_block_idx_(0), curr_kv_idx_(0) {
        if (!reader_ || reader_->GetBlockCount() == 0) return;
        data_ = reader_->LoadBlock(0);
    }

    bool Valid() {
        return !data_.empty() && curr_kv_idx_ < static_cast<int>(data_.size());
    }

    void Next() {
        if (!Valid()) return;

        curr_kv_idx_++;
        if (curr_kv_idx_ >= static_cast<int>(data_.size())) {
            // 🛑【核心修正】：固定按 LoadBlock 的物理块步长 (256) 推进，绝不依赖 data_.size()，防止索引偏移重叠
            curr_block_idx_ += 256; 
            curr_kv_idx_ = 0;
            data_.clear();

            if (reader_ && curr_block_idx_ < reader_->GetBlockCount()) {
                data_ = reader_->LoadBlock(curr_block_idx_);
            }
        }
    }

    int Key() {
        assert(Valid());
        return data_[curr_kv_idx_].first;
    }

    std::string Value() {
        assert(Valid());
        return data_[curr_kv_idx_].second;
    }

private:
    SSTableReader* reader_;
    int curr_block_idx_; 
    int curr_kv_idx_;    
    std::vector<std::pair<int, std::string>> data_;
};