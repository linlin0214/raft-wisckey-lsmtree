#pragma once

#include <cstring>
#include <string>
#include <string_view>
#include <cstdint>
#include <cassert>

class Slice {
public:
    Slice() : data_(""), size_(0) {}
    Slice(const char* data, size_t size) : data_(data), size_(size) {}
    Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}
    Slice(const char* s) : data_(s), size_(std::strlen(s)) {}
    Slice(std::string_view sv) : data_(sv.data()), size_(sv.size()) {}
    
    const char* data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    char operator[](size_t n) const {
        assert(n < size_);
        return data_[n];
    }
    std::string ToString() const { return std::string(data_, size_); }
    std::string_view ToStringView() const { return std::string_view(data_, size_); }

    int compare(const Slice& b) const {
        const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
        int r = std::memcmp(data_, b.data_, min_len);
        if (r == 0) {
            if (size_ < b.size_) r = -1;
            else if (size_ > b.size_) r = 1;
        } 
        return r;
    }

    int Compare(const Slice& b) const { return compare(b); }

    friend inline bool operator==(const Slice& x, const Slice& y) {
        return ((x.size() == y.size()) &&
                (std::memcmp(x.data(), y.data(), x.size()) == 0));
    }

    friend inline bool operator!=(const Slice& x, const Slice& y) {
        return !(x == y);
    }

    friend inline bool operator<(const Slice& x, const Slice& y) {
        return x.compare(y) < 0;
    }

    friend inline bool operator<=(const Slice& x, const Slice& y) {
        return x.compare(y) <= 0;
    }

    friend inline bool operator>(const Slice& x, const Slice& y) {
        return x.compare(y) > 0;
    }

    friend inline bool operator>=(const Slice& x, const Slice& y) {
        return x.compare(y) >= 0;
    }

private:
    const char* data_;
    size_t size_;
};