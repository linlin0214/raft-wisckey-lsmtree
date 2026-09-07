#pragma once
#include "Slice.h"
#include <vector>
#include <string_view>
#include <cmath>
#include <cstdint>

class BloomFilter {
public:
    BloomFilter() : k_(1), num_bits_(0) {}

    BloomFilter(int entries, int bits_per_key) {
        int bits = entries * bits_per_key;
        if (bits < 64) bits = 64; 
        
        bits_.resize((bits + 7) / 8, 0);
        num_bits_ = static_cast<uint32_t>(bits_.size() * 8);
        
        k_ = static_cast<int>(0.69 * bits_per_key); 
        if (k_ < 1) k_ = 1;
        if (k_ > 30) k_ = 30;
    }

    BloomFilter(const std::vector<uint8_t>& data) : bits_(data) {
        num_bits_ = static_cast<uint32_t>(bits_.size() * 8);
        k_ = static_cast<int>(0.69 * 10);
        if (k_ < 1) k_ = 1;
    }

    uint32_t BloomHash(std::string_view key) const {
        uint32_t h = 0x811c9dc5;
        for (char c : key) {
            h ^= static_cast<uint32_t>(static_cast<uint8_t>(c));
            h *= 0x01000193;
        }
        return h;
    }

    void Add(std::string_view key) {
        uint32_t h = BloomHash(key);
        const uint32_t delta = (h >> 17) | (h << 15); 
        for (int i = 0; i < k_; i++) {
            const uint32_t bit_pos = static_cast<uint32_t>((static_cast<uint64_t>(h) * num_bits_) >> 32);
            bits_[bit_pos / 8] |= (1 << (bit_pos % 8));
            h += delta;
        }
    }

    bool MightContain(std::string_view key) const {
        if (bits_.empty()) return true;
        uint32_t h = BloomHash(key);
        const uint32_t delta = (h >> 17) | (h << 15);
        for (int i = 0; i < k_; i++) {
            const uint32_t bit_pos = static_cast<uint32_t>((static_cast<uint64_t>(h) * num_bits_) >> 32);
            if (!(bits_[bit_pos / 8] & (1 << (bit_pos % 8)))) {
                return false; 
            }
            h += delta;
        }
        return true; 
    }

    //新增 Slice 重载
    void Add(const Slice& key) { Add(key.ToStringView()); }
    bool MightContain(const Slice& key) const { return MightContain(key.ToStringView()); }

    const std::vector<uint8_t>& GetData() const { return bits_; }

private:
    std::vector<uint8_t> bits_;
    int k_; 
    uint32_t num_bits_;
};