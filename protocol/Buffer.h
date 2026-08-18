#pragma once
#include <vector>
#include <string>
#include <string_view>
#include <cstddef>
#include <sys/uio.h>

class Buffer {
public:
    static constexpr size_t kCheapPrepend = 8;    // 刚性预留 8 字节 Protocol Header 头部空间
    static constexpr size_t kInitialSize = 1024;  // 初始容量 1KB

    explicit Buffer(size_t initial_size = kInitialSize);
    ~Buffer() = default;

    // 禁用拷贝构造与赋值
    Buffer(const Buffer&) = delete;
    Buffer& operator=(const Buffer&) = delete;

    void Swap(Buffer& rhs) noexcept;

    // 状态接口
    size_t ReadableBytes() const { return write_index_ - read_index_; }
    size_t WritableBytes() const { return buffer_.size() - write_index_; }
    size_t PrependableBytes() const { return read_index_; }

    const char* Peek() const { return Begin() + read_index_; }
    char* BeginWrite() { return Begin() + write_index_; }
    const char* BeginWrite() const { return Begin() + write_index_; }

    // 读指针推进与消费接口
    void Retrieve(size_t len);
    void RetrieveAll();
    std::string RetrieveAsString(size_t len);
    std::string RetrieveAllAsString();

    // 数据追加与前置插入接口
    void Append(const char* data, size_t len);
    void Append(std::string_view str);
    void Prepend(const void* data, size_t len);

    void EnsureWritableBytes(size_t len);

    // 核心高效网络读取：零拷贝 / readv 两段式散列读
    ssize_t ReadFd(int fd, int* saved_errno);

private:
    char* Begin() { return buffer_.data(); }
    const char* Begin() const { return buffer_.data(); }

    void MakeSpace(size_t len);

private:
    std::vector<char> buffer_;
    size_t read_index_;
    size_t write_index_;
};