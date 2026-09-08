#include "Connection.h"
#include "Channel.h"
#include "EventLoop.h" 
#include "TimingWheel.h"
#include <spdlog/spdlog.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <cerrno>

Connection::Connection(EventLoop* loop, int fd)
    : loop_(loop), 
      fd_(fd), 
      channel_(std::make_unique<Channel>(loop, fd)),
      state_(State::kConnecting) {
    
    // 禁用 Nagle 算法，消除 40ms 延迟
    int optval = 1;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &optval, static_cast<socklen_t>(sizeof(optval)));

    channel_->SetReadCallback([this]() { this->HandleRead(); });
    channel_->SetWriteCallback([this]() { this->HandleWrite(); });
    channel_->SetErrorCallback([this]() { this->HandleError(); });
    channel_->SetCloseCallback([this]() { this->HandleClose(); });
}

Connection::~Connection() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void Connection::ConnectionEstablished() {
    state_.store(State::kConnected, std::memory_order_release);
    channel_->Tie(shared_from_this());
    channel_->EnableReading();
    if (connection_callback_) {
        connection_callback_(shared_from_this());
    }
}

void Connection::ConnectionDestroyed() {
    loop_->AssertInLoopThread();
    State current_state = state_.load(std::memory_order_acquire);
    
    // 如果已经是 kDisconnected，说明 HandleClose 已经注销过 Channel，立刻退出
    if (current_state == State::kDisconnected) {
        return;
    }

    state_.store(State::kDisconnected, std::memory_order_release);
    channel_->DisableAll();
    channel_->Remove();
}

void Connection::ForceClose() {
    if (state_.load(std::memory_order_acquire) == State::kConnected) {
        if (loop_->IsInLoopThread()) {
            // 🚀 主动向对端发送 FIN，关闭套接字读写，立即解除远端客户端阻塞
            if (fd_ >= 0) {
                ::shutdown(fd_, SHUT_RDWR);
            }
            HandleClose();
        } else {
            auto self = shared_from_this();
            loop_->QueueInLoop([self]() {
                self->ForceClose();
            });
        }
    }
}

void Connection::Send(std::string_view data) {
    if (state_.load(std::memory_order_acquire) != State::kConnected) return;

    if (loop_->IsInLoopThread()) {
        SendInLoop(data);
    } else {
        auto self = shared_from_this();
        std::string message(data);
        loop_->QueueInLoop([self, message = std::move(message)]() {
            self->SendInLoop(message);
        });
    }
}

void Connection::SendInLoop(std::string_view data) {
    loop_->AssertInLoopThread();
    if (state_.load(std::memory_order_acquire) != State::kConnected) return;

    ssize_t nwrote = 0;
    size_t remaining = data.size();
    bool fault_error = false;

    if (!channel_->IsWriting() && output_buffer_.ReadableBytes() == 0) {
        nwrote = ::write(fd_, data.data(), data.size());
        if (nwrote >= 0) {
            remaining -= nwrote;
            if (remaining == 0) {
                return;
            }
        } else {
            nwrote = 0;
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                spdlog::error("[Connection] 物理套接字直驱写入报错 fd: {}, errno: {}", fd_, errno);
                fault_error = true;
                HandleError();
                return; 
            }
        }
    }

    if (!fault_error && remaining > 0 && state_.load(std::memory_order_acquire) == State::kConnected) {
        output_buffer_.Append(data.data() + nwrote, remaining);
        if (!channel_->IsWriting()) {
            channel_->EnableWriting();
        }
    }
}

void Connection::HandleRead() {
    loop_->AssertInLoopThread();
    if (state_.load(std::memory_order_acquire) != State::kConnected) return;

    char buf[65536];
    ssize_t n = ::read(fd_, buf, sizeof(buf));
    if (n > 0) {
        // 🚀 严谨调整：仅在真正接收到有效网络载荷时才延展租约
        auto wheel = loop_->GetContext<TimingWheel>("timing_wheel");
        if (wheel) {
            wheel->Touch(shared_from_this());
        }
        input_buffer_.Append(buf, n);
    } else if (n == 0) {
        HandleClose();
        return; 
    } else {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            spdlog::error("[Connection] 套接字读取异常 fd: {}, errno: {}", fd_, errno);
            HandleError();
            return; 
        }
    }

    if (state_.load(std::memory_order_acquire) == State::kConnected && message_callback_ && input_buffer_.ReadableBytes() > 0) {
        message_callback_(shared_from_this(), &input_buffer_);
    }
}

void Connection::HandleWrite() {
    loop_->AssertInLoopThread();
    if (state_.load(std::memory_order_acquire) != State::kConnected) return;

    if (channel_->IsWriting()) {
        while (output_buffer_.ReadableBytes() > 0) {
            ssize_t n = ::write(fd_, output_buffer_.Peek(), output_buffer_.ReadableBytes());
            if (n > 0) {
                output_buffer_.Retrieve(n); 
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                spdlog::error("[Connection] 异步写冲刷失败 fd: {}, errno: {}", fd_, errno);
                HandleError();
                return; 
            }
        }
        
        if (state_.load(std::memory_order_acquire) == State::kConnected && output_buffer_.ReadableBytes() == 0) {
            channel_->DisableWriting();
        }
    }
}

void Connection::HandleClose() {
    loop_->AssertInLoopThread();
    State expected = state_.load(std::memory_order_acquire);
    
    if (expected != State::kDisconnected) {
        state_.store(State::kDisconnected, std::memory_order_release);
        
        channel_->DisableAll();
        channel_->Remove();

        ConnectionPtr guard(shared_from_this());
        if (close_callback_) {
            auto cb = close_callback_;
            loop_->QueueInLoop([guard, cb]() {
                if (cb) {
                    cb(guard);
                }
            });
        }
    }
}

void Connection::HandleError() {
    loop_->AssertInLoopThread();
    State current_state = state_.load(std::memory_order_acquire);
    if (current_state == State::kDisconnected) {
        return;
    }
    spdlog::error("[Connection] 套接字触发硬报错，执行关闭程序 fd: {}", fd_);
    HandleClose();
}