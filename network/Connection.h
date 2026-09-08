#pragma once

#include "Buffer.h"
#include <memory>
#include <functional>
#include <string_view>
#include <atomic>

class EventLoop;
class Channel;

class Connection : public std::enable_shared_from_this<Connection> {
public:
    enum class State {
        kConnecting,
        kConnected,
        kDisconnected
    };

    using ConnectionPtr = std::shared_ptr<Connection>;
    using ConnectionCallback = std::function<void(const ConnectionPtr&)>;
    using MessageCallback = std::function<void(const ConnectionPtr&, Buffer*)>;
    using CloseCallback = std::function<void(const ConnectionPtr&)>;

public:
    Connection(EventLoop* loop, int fd);
    ~Connection();

    // 严禁拷贝与赋值
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    // 核心 Getter
    EventLoop* GetLoop() const { return loop_; }
    int GetFd() const { return fd_; }
    bool IsConnected() const { return state_.load(std::memory_order_acquire) == State::kConnected; }

    // 回调注入
    void SetConnectionCallback(ConnectionCallback cb) { connection_callback_ = std::move(cb); }
    void SetMessageCallback(MessageCallback cb) { message_callback_ = std::move(cb); }
    void SetCloseCallback(CloseCallback cb) { close_callback_ = std::move(cb); }

    // 生命周期管理
    void ConnectionEstablished();
    void ConnectionDestroyed();

    // 数据发送与 Buffer 访问
    void Send(std::string_view data);
    Buffer* GetInputBuffer() { return &input_buffer_; }
    Buffer* GetOutputBuffer() { return &output_buffer_; }
    
    void ForceClose(); //  供时间轮超时触发强制关闭

    // 上下文存取 (弱引用保存，杜绝循环引用导致对象无法释放)
    void SetCustomContext(const std::shared_ptr<void>& ctx) { custom_context_ = ctx; }
    template <typename T>
    std::shared_ptr<T> GetCustomContext() const {
        return std::static_pointer_cast<T>(custom_context_.lock());
    }

private:
    void SendInLoop(std::string_view data);
    void HandleRead();
    void HandleWrite();
    void HandleClose();
    void HandleError();

private:
    EventLoop* loop_;                  // 所属 EventLoop 句柄
    int fd_;                           // 物理套接字句柄
    std::unique_ptr<Channel> channel_; // 打理当前 fd_ 读写事件的 Channel
    std::atomic<State> state_;         // 状态机：kConnecting / kConnected / kDisconnected

    Buffer input_buffer_;              // 应用层接收缓冲区
    Buffer output_buffer_;             // 应用层发送缓冲区

    ConnectionCallback connection_callback_; // 连接建立/断开上层通知闭包
    MessageCallback message_callback_;       // 接收到新消息上层切包闭包
    CloseCallback close_callback_;           // 内部注销回调 (指向 TcpServer::RemoveConnection)

    std::weak_ptr<void> custom_context_;
};