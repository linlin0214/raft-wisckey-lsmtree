#pragma once

#include "SSTableBuilder.h"
#include "BloomFilter.h"
#include "LRUCache.h"
#include "Slice.h"
#include <vector>
#include <string>
#include <algorithm>
#include <cstdint>
#include <cassert>
#include <unistd.h>
#include <fcntl.h>
#include <stdexcept>
#include <cstring>

class SSTableReader {
public:
    explicit SSTableReader(const std::string& filename) 
        : filename_(filename), bloom_filter_() {
        fd_ = ::open(filename.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw std::runtime_error("无法打开 SSTable 文件: " + filename);
        }
        LoadMetadata();
    }

    ~SSTableReader() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;

    bool MightContain(const Slice& key) const {
        return bloom_filter_.MightContain(key);
    }

    std::string Search(const Slice& key, ShardedLRUCache* cache = nullptr) {
        if (key < Slice(min_key_) || key > Slice(max_key_)) return ""; 
        if (!MightContain(key)) return "";

        std::string cache_key;
        if (cache) {
            cache_key = filename_ + "_" + key.ToString();
            std::string cached_val;
            if (cache->Get(cache_key, cached_val)) {
                return cached_val; 
            }
        }

        auto it = std::lower_bound(index_.begin(), index_.end(), key, 
            [](const IndexEntry& a, const Slice& k) { 
                return Slice(a.key) < k; 
            });

        if (it != index_.end() && Slice(it->key) == key) {
            std::string disk_val = ReadValueAt(it->offset);
            if (cache && !disk_val.empty()) {
                cache->Put(cache_key, disk_val);
            }
            return disk_val;
        }
        return ""; 
    }

    std::string GetMinKey() const { return min_key_; }
    std::string GetMaxKey() const { return max_key_; }
    int GetBlockCount() const { return static_cast<int>(index_.size()); }
    std::string GetFilename() const { return filename_; }

    std::vector<std::pair<std::string, std::string>> LoadBlock(int start_index) {
        std::vector<std::pair<std::string, std::string>> res;
        if (start_index < 0 || start_index >= static_cast<int>(index_.size())) return res;

        int end_index = std::min(static_cast<int>(index_.size()), start_index + 256);
        res.reserve(end_index - start_index);

        for (int i = start_index; i < end_index; ++i) {
            uint32_t curr_offset = index_[i].offset;
            char header[6];
            if (::pread(fd_, header, 6, curr_offset) != 6) continue;

            uint16_t k_len;
            uint32_t v_len;
            std::memcpy(&k_len, header, 2);
            std::memcpy(&v_len, header + 2, 4);

            if (v_len > 1024 * 1024) continue;

            std::string key_str(k_len, '\0');
            if (k_len > 0) {
                if (::pread(fd_, &key_str[0], k_len, curr_offset + 6) != static_cast<ssize_t>(k_len)) continue;
            }

            std::string vlog_ptr_raw(v_len, '\0');
            if (v_len > 0) {
                if (::pread(fd_, &vlog_ptr_raw[0], v_len, curr_offset + 6 + k_len) != static_cast<ssize_t>(v_len)) continue;
            }

            res.push_back({std::move(key_str), std::move(vlog_ptr_raw)});
        }
        return res;
    }

private:
    std::string filename_;
    int fd_{-1};     
    std::vector<IndexEntry> index_;
    BloomFilter bloom_filter_; 
    std::string min_key_;  
    std::string max_key_;  

    void LoadMetadata() {
        off_t file_size = ::lseek(fd_, 0, SEEK_END);
        if (file_size == -1 || file_size < 16) return;

        char footer[16];
        if (::pread(fd_, footer, 16, file_size - 16) != 16) return;

        uint32_t index_offset, index_bytes, bloom_offset, bloom_bytes;
        std::memcpy(&index_offset, footer, 4);
        std::memcpy(&index_bytes, footer + 4, 4);
        std::memcpy(&bloom_offset, footer + 8, 4);
        std::memcpy(&bloom_bytes, footer + 12, 4);

        if (index_offset + index_bytes > static_cast<uint32_t>(file_size) ||
            bloom_offset + bloom_bytes > static_cast<uint32_t>(file_size)) return;

        std::vector<char> index_buf(index_bytes);
        if (::pread(fd_, index_buf.data(), index_bytes, index_offset) == static_cast<ssize_t>(index_bytes)) {
            const char* ptr = index_buf.data();
            const char* end = ptr + index_bytes;

            if (ptr + 4 <= end) {
                uint32_t count = 0;
                std::memcpy(&count, ptr, 4);
                ptr += 4;

                if (ptr + 2 <= end) {
                    uint16_t min_len = 0;
                    std::memcpy(&min_len, ptr, 2);
                    ptr += 2;
                    if (ptr + min_len <= end) {
                        min_key_.assign(ptr, min_len);
                        ptr += min_len;
                    }
                }

                if (ptr + 2 <= end) {
                    uint16_t max_len = 0;
                    std::memcpy(&max_len, ptr, 2);
                    ptr += 2;
                    if (ptr + max_len <= end) {
                        max_key_.assign(ptr, max_len);
                        ptr += max_len;
                    }
                }

                index_.reserve(count);
                for (uint32_t i = 0; i < count && ptr < end; ++i) {
                    if (ptr + 2 > end) break;
                    uint16_t klen = 0;
                    std::memcpy(&klen, ptr, 2);
                    ptr += 2;

                    if (ptr + klen + 4 > end) break;
                    std::string k(ptr, klen);
                    ptr += klen;

                    uint32_t off = 0;
                    std::memcpy(&off, ptr, 4);
                    ptr += 4;

                    index_.push_back(IndexEntry{std::move(k), off});
                }
            }
        }

        if (bloom_bytes > 0) {
            std::vector<uint8_t> bf_data(bloom_bytes);
            if (::pread(fd_, reinterpret_cast<char*>(bf_data.data()), bloom_bytes, bloom_offset) == static_cast<ssize_t>(bloom_bytes)) {
                bloom_filter_ = BloomFilter(bf_data);
            }
        }
    }

    std::string ReadValueAt(uint32_t offset) {
        char header[6];
        if (::pread(fd_, header, 6, offset) != 6) return ""; 

        uint16_t k_len;
        uint32_t v_len;
        std::memcpy(&k_len, header, 2);
        std::memcpy(&v_len, header + 2, 4);

        if (v_len == 0 || v_len > 1024 * 1024) return ""; 

        std::string vlog_ptr_raw(v_len, '\0');
        if (::pread(fd_, &vlog_ptr_raw[0], v_len, offset + 6 + k_len) != static_cast<ssize_t>(v_len)) {
            return "";
        }
        return vlog_ptr_raw; 
    }
};

class SSTableIterator {
public:
    explicit SSTableIterator(SSTableReader* reader)
        : reader_(reader), curr_block_idx_(0), curr_kv_idx_(0) {
        if (!reader_ || reader_->GetBlockCount() == 0) return;
        data_ = reader_->LoadBlock(0);
    }

    bool Valid() const {
        return !data_.empty() && curr_kv_idx_ < static_cast<int>(data_.size());
    }

    void Next() {
        if (!Valid()) return;

        curr_kv_idx_++;
        if (curr_kv_idx_ >= static_cast<int>(data_.size())) {
            curr_block_idx_ += 256; 
            curr_kv_idx_ = 0;
            data_.clear();

            if (reader_ && curr_block_idx_ < reader_->GetBlockCount()) {
                data_ = reader_->LoadBlock(curr_block_idx_);
            }
        }
    }

    // 返回 Slice 保证零拷贝，且其生命周期与 SSTableIterator 内部缓存块一致
    Slice Key() const {
        assert(Valid());
        return Slice(data_[curr_kv_idx_].first);
    }

    std::string Value() const {
        assert(Valid());
        return data_[curr_kv_idx_].second;
    }

private:
    SSTableReader* reader_;
    int curr_block_idx_; 
    int curr_kv_idx_;    
    std::vector<std::pair<std::string, std::string>> data_;
};