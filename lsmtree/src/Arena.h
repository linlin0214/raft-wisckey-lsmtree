#pragma once

#include <vector>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cassert>

class Arena {
public:
    Arena() : ptr_(nullptr), remaining_(0), memory_usage_(0) {
        //  预分配指针数组空间，避免 vector 频繁扩容带来的瞬时延迟
        blocks_.reserve(256); 
        
        //  将初始块提升到 256KB，大幅减少小碎件分配时对系统级 new 的调用
        const size_t kInitBlockSize = 256 * 1024; 
        ptr_ = Allocatenewblock(kInitBlockSize);
        remaining_ = kInitBlockSize;
    }

    ~Arena() {
        for (auto b : blocks_) {
            delete[] b;
        }
    }

    //  极致内联分配，榨干 CPU 寄存器效率
    inline char* Allocate(size_t bytes) {
        //  大多数情况下的命中分支（当前块剩余空间足够）
        if (bytes <= remaining_) {
            char* res = ptr_;
            ptr_ += bytes;
            remaining_ -= bytes;
            return res;
        }
        return Allocatefallback(bytes);
    }

    char* Allocatealigned(size_t bytes);
    void Reset();
    
    
    size_t memory_usage() const { 
        return memory_usage_.load(std::memory_order_relaxed); 
    }

    //池禁止拷贝和赋值
    Arena(const Arena&) = delete;
    Arena(Arena&&) = delete;
    Arena& operator=(Arena&&) = delete;
    Arena& operator=(const Arena&) = delete;

    //O(1)
    // 当活跃 memtable 写满之后，无需申请新内存，直接与下一个空的 memtable 极速对调底层 Arena 空间
    void Swap(Arena& other) {
        std::swap(this->ptr_, other.ptr_);
        std::swap(this->remaining_, other.remaining_);
        std::swap(this->blocks_, other.blocks_); 

        //  std::atomic 必须手动 load 和 store，无法直接 std::swap
        auto temp_usage = this->memory_usage_.load(std::memory_order_relaxed);
        this->memory_usage_.store(other.memory_usage_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        other.memory_usage_.store(temp_usage, std::memory_order_relaxed);
    }

private:
    char* Allocatefallback(size_t bytes);
    char* Allocatenewblock(size_t block_bytes);

    char* ptr_;
    size_t remaining_;
    std::vector<char*> blocks_;
    std::atomic<size_t> memory_usage_;
};

//  高效对齐函数 (前提: align 必须是 2 的幂，利用位运算消除取模硬件开销)
inline char* AlignPtr(char* ptr, size_t align) {
    uintptr_t cur = reinterpret_cast<uintptr_t>(ptr);
    uintptr_t aligned = (cur + align - 1) & ~(align - 1);
    return reinterpret_cast<char*>(aligned);
}

inline char* Arena::Allocatealigned(size_t bytes) {
    const size_t align = sizeof(void*);
    char* aligned_ptr = AlignPtr(ptr_, align);
    size_t padding = aligned_ptr - ptr_;
    size_t needed = bytes + padding;

    if (needed <= remaining_) {
        ptr_ += needed;
        remaining_ -= needed;
        return aligned_ptr;
    }
    //  对齐失败说明当前块不够了，直接走 fallback 分配新内存块
    return Allocatefallback(bytes);
}

inline char* Arena::Allocatefallback(size_t bytes) {
    //  超大块数据（大于标准块的 1/4）直接单独开辟，不浪费标准块的空间
    if (bytes > 256 * 1024 / 4) {
        return Allocatenewblock(bytes);
    }

    //  默认块大小提升至 256KB
    const size_t kBlockSize = 256 * 1024;
    ptr_ = Allocatenewblock(kBlockSize);
    remaining_ = kBlockSize;

    char* res = ptr_;
    ptr_ += bytes;
    remaining_ -= bytes;
    return res;
}

inline char* Arena::Allocatenewblock(size_t block_bytes) {
    char* block = new char[block_bytes];
    blocks_.push_back(block);
    //  更新内存占用（包括 vector 指针等元数据开销的粗略估算）
    memory_usage_.fetch_add(block_bytes + sizeof(char*), std::memory_order_relaxed);
    return block;
}

inline void Arena::Reset() {
    //  物理销毁全部旧内存，重新分配初始块
    for (auto b : blocks_) {
        delete[] b;
    }
    blocks_.clear();
    
    memory_usage_.store(0, std::memory_order_relaxed);
    
    const size_t kInitBlockSize = 256 * 1024;
    ptr_ = Allocatenewblock(kInitBlockSize);
    remaining_ = kInitBlockSize;
}