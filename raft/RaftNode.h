#pragma once

#include "lsmtree/src/RaftStorageAdapter.h"
#include "lsmtree/src/Slice.h"
#include "lsmtree/src/SPSCQueue.h"
#include "RaftCore.h"
#include "protocol/RaftRpc.h"
#include "network/EventLoop.h"
#include "network/TimerQueue.h"
#include "network/Connection.h"
#include "network/RpcClient.h"

#include <cstddef>
#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <thread>
#include <chrono>

namespace raft_rpc {
class RaftDispatcher;
}

class RaftNode : public std::enable_shared_from_this<RaftNode> {
public:
    struct NodeStatus {
        uint32_t node_id;
        std::string role;
        uint64_t term;
        uint32_t leader_id;
        uint64_t last_log_index;
        uint64_t commit_index;
        uint64_t stabled_index;
        uint64_t last_applied;
    };
    NodeStatus GetStatus();
    
    struct Peer {
        uint32_t id;
        std::string ip;
        uint16_t port;
        std::shared_ptr<RpcClient> client;
        std::weak_ptr<Connection> outbound_conn{};
        std::weak_ptr<Connection> inbound_conn{};

        std::shared_ptr<Connection> GetActiveConnection() const {
            if (auto conn = outbound_conn.lock()) {
                if (conn->IsConnected()) {
                    return conn;
                }
            }
            if (auto conn = inbound_conn.lock()) {
                if (conn->IsConnected()) {
                    return conn;
                }
            }
            return nullptr;
        }
    };

public:
    static constexpr uint64_t kSnapshotCountThreshold = 500000;
    static constexpr uint64_t kSnapshotIntervalSec   = 300;

public:
    RaftNode(
        EventLoop* loop,
        uint32_t node_id,
        const std::vector<uint32_t>& peer_ids,
        raft_rpc::RaftDispatcher* dispatcher,
        const std::string& storage_dir = ""
    );

    ~RaftNode();

public:
    void Start();

    void AddPeer(
        uint32_t peer_id,
        const std::string& ip,
        uint16_t port
    );

    bool Propose(
        const Slice& key,
        std::string_view value,
        const std::shared_ptr<Connection>& client,
        uint64_t req_id = 0
    );

    // 批处理事务提案通道
    bool ProposeBatch(
        std::string_view full_packet,
        const std::shared_ptr<Connection>& client,
        uint64_t req_id = 0
    );

    bool ProposeRead(
        const Slice& key,
        const std::shared_ptr<Connection>& client,
        uint64_t req_id = 0
    );

public:
    void HandlePreVote(
        const std::shared_ptr<Connection>& conn,
        uint64_t req_id,
        const raft_rpc::PreVoteArgs& args
    );

    void HandlePreVoteReply(
        uint64_t req_id,
        const raft_rpc::PreVoteReply& reply
    );

    void HandleAppendEntries(
        const std::shared_ptr<Connection>& conn,
        uint64_t req_id,
        const raft_rpc::AppendEntriesArgs& args
    );

    void HandleAppendEntriesReply(
        uint64_t req_id,
        const raft_rpc::AppendEntriesReply& reply
    );

    void HandleRequestVote(
        const std::shared_ptr<Connection>& conn,
        uint64_t req_id,
        const raft_rpc::RequestVoteArgs& args
    );

    void HandleRequestVoteReply(
        uint64_t req_id,
        const raft_rpc::RequestVoteReply& reply
    );

    void HandleInstallSnapshot(
        const std::shared_ptr<Connection>& conn,
        uint64_t req_id,
        const raft_rpc::InstallSnapshotArgs& args
    );

    void HandleInstallSnapshotReply(
        uint64_t req_id,
        const raft_rpc::InstallSnapshotReply& reply
    );

    uint32_t GetNodeId() const { return node_id_; }

    uint32_t GetLeaderId() {
        std::lock_guard<std::mutex> lock(core_mtx_);
        return core_ ? core_->GetLeaderId() : RaftCore::kNoLeader;
    }

    void GracefulShutdown(std::function<void()> on_complete);

private:
    void OnTick();
    void ScheduleCheckAndAdvance();
    void DoCheckAndAdvance();
    void CheckAndAdvance();
    void OnPersistFinished(Ready rd, AdvanceState state);
    void StorageThreadLoop();

    void CheckAndTriggerSnapshot(uint64_t current_applied);
    void TriggerSnapshot(uint64_t compact_index);

    void SendOutboundMessage(const OutboundMessage& msg);
    void UpdateInboundConnection(uint32_t peer_id, const std::shared_ptr<Connection>& conn);

private:
    struct PendingClientRead {
        std::string ctx;
        std::string key;
        uint64_t client_req_id{0};
        std::weak_ptr<Connection> conn;
        std::chrono::steady_clock::time_point start_time;
    };

    struct WaitingApplyRead {
        uint64_t read_index{0};
        std::string key;
        uint64_t client_req_id{0};
        std::weak_ptr<Connection> conn;
    };

    void ProcessReadStates(const std::vector<ReadState>& read_states);
    void CheckWaitingAppliedReads(uint64_t current_applied);
    void CleanupExpiredReads();

    std::atomic<uint64_t> next_read_ctx_id_{1};
    std::unordered_map<std::string, PendingClientRead> pending_client_reads_;
    std::vector<WaitingApplyRead> waiting_applied_reads_;

private:
    EventLoop* loop_;
    uint32_t node_id_;
    raft_rpc::RaftDispatcher* dispatcher_;
    
    std::vector<Peer> peers_;
    std::unique_ptr<RaftCore> core_;
    std::mutex core_mtx_;

    RaftStorageAdapter storage_adapter_;

    std::thread storage_thread_;
    SPSCQueue<Ready, 65536> storage_queue_;
    std::atomic<bool> storage_running_{false};

    std::atomic<bool> is_ready_scheduled_{false};
    std::atomic<bool> is_persisting_{false};
    std::atomic<bool> is_snapshotting_{false};

    uint64_t last_snapshot_index_{0};
    uint64_t last_snapshot_time_sec_{0};

    TimerQueue tick_timer_;

    std::mutex client_mtx_;
    
    struct PendingClientWrite {
        std::shared_ptr<Connection> conn;
        std::chrono::steady_clock::time_point start_time;
    };
    std::unordered_map<uint64_t, PendingClientWrite> client_wait_list_;
    std::unordered_map<uint64_t, std::weak_ptr<Connection>> pending_replies_;

    std::atomic<bool> is_shutting_down_{false};
};