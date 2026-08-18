#pragma once
#include "network/Connection.h"
#include "protocol/Buffer.h"
#include "protocol/Codec.h"
#include "protocol/RaftRpc.h"
#include <functional>
#include <memory>
#include <spdlog/spdlog.h>

namespace raft_rpc {

class RaftDispatcher {
public:
    // 向上层 Raft 核心暴露，如果不使用回调Dispatcher 必须持有一个 RaftNode 的指针或引用
    using VoteRequestCallback      = std::function<void(const std::shared_ptr<Connection>&, uint64_t req_id, const RequestVoteArgs&)>;
    using VoteReplyCallback        = std::function<void(uint64_t req_id, const RequestVoteReply&)>;
    using HeartbeatRequestCallback = std::function<void(const std::shared_ptr<Connection>&, uint64_t req_id, const AppendEntriesArgs&)>;
    using HeartbeatReplyCallback   = std::function<void(uint64_t req_id, const AppendEntriesReply&)>;
    using SnapshotRequestCallback  = std::function<void(const std::shared_ptr<Connection>&, uint64_t req_id, const InstallSnapshotArgs&)>;
    using SnapshotReplyCallback    = std::function<void(uint64_t req_id, const InstallSnapshotReply&)>;

private:
    VoteRequestCallback      vote_req_cb_;
    VoteReplyCallback        vote_reply_cb_;
    HeartbeatRequestCallback heartbeat_req_cb_;   
    HeartbeatReplyCallback   heartbeat_reply_cb_; 
    SnapshotRequestCallback  snapshot_req_cb_;    
    SnapshotReplyCallback    snapshot_reply_cb_;  

public:
    RaftDispatcher() = default;
    ~RaftDispatcher() = default;

    // 禁用拷贝构造与赋值运算符
    RaftDispatcher(const RaftDispatcher&) = delete;
    RaftDispatcher& operator=(const RaftDispatcher&) = delete;

    // 注册业务触点
    //std::move 的本质是：以极低的开销，将 cb 内部持有的“如堆内存指针的所有权”转移给成员变量，而不是深拷贝一份新的资源
    void SetVoteRequestCallback(VoteRequestCallback cb) { vote_req_cb_ = std::move(cb); }
    void SetVoteReplyCallback(VoteReplyCallback cb) { vote_reply_cb_ = std::move(cb); }
    void SetHeartbeatRequestCallback(HeartbeatRequestCallback cb) { heartbeat_req_cb_ = std::move(cb); } 
    void SetHeartbeatReplyCallback(HeartbeatReplyCallback cb) { heartbeat_reply_cb_ = std::move(cb); }   
    void SetSnapshotRequestCallback(SnapshotRequestCallback cb) { snapshot_req_cb_ = std::move(cb); }
    void SetSnapshotReplyCallback(SnapshotReplyCallback cb) { snapshot_reply_cb_ = std::move(cb); }

    // 控制反转核心枢纽
    void OnMessage(const std::shared_ptr<Connection>& conn, Buffer* buf) {
        // 1. 循环调用 Codec 精准切包，彻底解决高并发 TCP 粘包问题
        while (true) {
            auto packet = Codec::Parse(buf);
            if (!packet.has_value()) {
                break; // 半包发生，或 Buffer 被吃空，安全退出
            }

            uint8_t opcode = packet->header.opcode;
            uint64_t req_id = packet->header.req_id;
            const std::string& body = packet->body;

            spdlog::debug("[Dispatcher] 捕获网络报文 Opcode: 0x{:02X}, ReqID: {}", opcode, req_id);

            // 捕获反序列化与回调异常，保护 EventLoop 线程
            try {
                switch (opcode) {
                    case RaftOpcode::kRequestVote: {
                        if (vote_req_cb_) {
                            RequestVoteArgs args = RequestVoteArgs::Deserialize(body);
                            vote_req_cb_(conn, req_id, args);
                        } else {
                            spdlog::warn("[Dispatcher] 收到 kRequestVote 但未注册 vote_req_cb_！");
                        }
                        break;
                    }
                    case RaftOpcode::kRequestVoteReply: {
                        if (vote_reply_cb_) {
                            RequestVoteReply reply = RequestVoteReply::Deserialize(body);
                            vote_reply_cb_(req_id, reply);
                        } else {
                            spdlog::warn("[Dispatcher] 收到 kRequestVoteReply 但未注册 vote_reply_cb_！");
                        }
                        break;
                    }
                    case RaftOpcode::kAppendEntries: {
                        if (heartbeat_req_cb_) {
                            AppendEntriesArgs args = AppendEntriesArgs::Deserialize(body);
                            heartbeat_req_cb_(conn, req_id, args);
                        } else {
                            spdlog::warn("[Dispatcher] 收到 kAppendEntries 但未注册 heartbeat_req_cb_！");
                        }
                        break;
                    }
                    case RaftOpcode::kAppendEntriesReply: {
                        if (heartbeat_reply_cb_) {
                            AppendEntriesReply reply = AppendEntriesReply::Deserialize(body);
                            heartbeat_reply_cb_(req_id, reply);
                        } else {
                            spdlog::warn("[Dispatcher] 收到 kAppendEntriesReply 但未注册 heartbeat_reply_cb_！");
                        }
                        break;
                    }
                    case RaftOpcode::kInstallSnapshot: {
                        if (snapshot_req_cb_) {
                            InstallSnapshotArgs args = InstallSnapshotArgs::Deserialize(body);
                            snapshot_req_cb_(conn, req_id, args);
                        } else {
                            spdlog::warn("[Dispatcher] 收到 kInstallSnapshot 但未注册 snapshot_req_cb_！");
                        }
                        break;
                    }
                    case RaftOpcode::kInstallSnapshotReply: {
                        if (snapshot_reply_cb_) {
                            InstallSnapshotReply reply = InstallSnapshotReply::Deserialize(body);
                            snapshot_reply_cb_(req_id, reply);
                        } else {
                            spdlog::warn("[Dispatcher] 收到 kInstallSnapshotReply 但未注册 snapshot_reply_cb_！");
                        }
                        break;
                    }
                    default:
                        spdlog::error("[Dispatcher] 侦测到未知的非 Raft 业务异形操作码: 0x{:02X}，强行丢弃。", opcode);
                        break;
                }
            } catch (const std::exception& e) {
                spdlog::error("[Dispatcher] 处理报文发生异常 Opcode: 0x{:02X}, ReqID: {}, err: {}", opcode, req_id, e.what());
            }
        }
    }
};

} // namespace raft_rpc