#include "Buffer.h"
#include <cstring>
#include <cerrno>
#include <unistd.h>

Buffer::Buffer(size_t initial_size)
    : buffer_(kCheapPrepend + initial_size),
      read_index_(kCheapPrepend),
      write_index_(kCheapPrepend) {}

void Buffer::Swap(Buffer& rhs) noexcept {
    buffer_.swap(rhs.buffer_);
    std::swap(read_index_, rhs.read_index_);
    std::swap(write_index_, rhs.write_index_);
}

void Buffer::Retrieve(size_t len) {
    if (len < ReadableBytes()) {
        read_index_ += len;
    } else {
        RetrieveAll();
    }
}

void Buffer::RetrieveAll() {
    read_index_ = kCheapPrepend;
    write_index_ = kCheapPrepend;
}

std::string Buffer::RetrieveAsString(size_t len) {
    size_t readable = ReadableBytes();
    size_t actual_len = std::min(len, readable);
    std::string result(Peek(), actual_len);
    Retrieve(actual_len);
    return result;
}

std::string Buffer::RetrieveAllAsString() {
    return RetrieveAsString(ReadableBytes());
}

void Buffer::Append(const char* data, size_t len) {
    EnsureWritableBytes(len);
    std::memcpy(BeginWrite(), data, len);
    write_index_ += len;
}

void Buffer::Append(std::string_view str) {
    Append(str.data(), str.size());
}

void Buffer::Prepend(const void* data, size_t len) {
    if (len > PrependableBytes()) {
        return; // 防御：Prepend 空间不足
    }
    read_index_ -= len;
    std::memcpy(Begin() + read_index_, data, len);
}

void Buffer::EnsureWritableBytes(size_t len) {
    if (WritableBytes() < len) {
        MakeSpace(len);
    }
}

void Buffer::MakeSpace(size_t len) {
    if (WritableBytes() + PrependableBytes() < len + kCheapPrepend) {
        // 总空间彻底不足：物理扩展 vector 容量
        buffer_.resize(write_index_ + len);
    } else {
        // 优化：利用已读废弃空间，原地 memmove 平移归位，复用内存
        size_t readable = ReadableBytes();
        std::memmove(Begin() + kCheapPrepend, Begin() + read_index_, readable);
        read_index_ = kCheapPrepend;
        write_index_ = read_index_ + readable;
    }
}

ssize_t Buffer::ReadFd(int fd, int* saved_errno) {
    char extra_buf[65536]; // 64KB 栈上临时缓冲区
    struct iovec vec[2];
    const size_t writable = WritableBytes();

    vec[0].iov_base = BeginWrite();
    vec[0].iov_len = writable;
    vec[1].iov_base = extra_buf;
    vec[1].iov_len = sizeof(extra_buf);

    // 如果 Buffer 内部空闲空间已足够容纳 64KB，则不使用额外栈空间
    const int iovcnt = (writable < sizeof(extra_buf)) ? 2 : 1;
    const ssize_t n = ::readv(fd, vec, iovcnt);

    if (n < 0) {
        *saved_errno = errno;
    } else if (static_cast<size_t>(n) <= writable) {
        write_index_ += n;
    } else {
        write_index_ = buffer_.size();
        Append(extra_buf, n - writable); // 溢出部分追加入 Buffer
    }
    return n;
}