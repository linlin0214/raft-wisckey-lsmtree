#pragma once

#include "RaftTypes.h"
#include "RaftLog.h"
#include "protocol/RaftRpc.h"
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <random>
#include <cstdint>
#include <string>

class RaftCore {
public:
    static constexpr uint32_t kNoLeader = UINT32_MAX;

    RaftCore(uint32_t node_id, const std::vector<uint32_t>& peer_ids, uint64_t random_seed = 0);
    ~RaftCore() = default;

    void Restore(const HardState& hs, std::vector<raft_rpc::LogEntry> logs);

    void Tick();

    void Step(uint32_t from_node, raft_rpc::RaftOpcode opcode, uint64_t req_id, const std::string& payload);
    bool Propose(const std::string& command);

    bool HasReady() const;
    Ready PopReady();
    void Advance(uint64_t stabled_index, uint64_t applied_index);

    RaftRole GetRole() const { return role_; }
    bool IsLeader() const { return role_ == RaftRole::kLeader; }
    uint64_t GetCurrentTerm() const { return state_.term; }
    uint64_t GetCommitIndex() const { return raft_log_.commit_index(); }
    uint32_t GetLeaderId() const { return leader_id_; }
    uint64_t GetLastLogIndex() const { return raft_log_.GetLastIndex(); }
    uint64_t GetStabledIndex() const { return raft_log_.stabled_index(); }
    uint64_t GetLastApplied() const { return raft_log_.last_applied(); }

private:
    void BecomeFollower(uint64_t term, int32_t voted_for);
    void BecomeCandidate();
    void BecomeLeader();

    void ResetElectionTimer();
    void TickElection();
    void TickHeartbeat();

    void HandleAppendEntries(uint32_t from, uint64_t req_id, const std::string& payload);
    void HandleAppendEntriesReply(uint32_t from, uint64_t req_id, const std::string& payload);
    void HandleRequestVote(uint32_t from, uint64_t req_id, const std::string& payload);
    void HandleRequestVoteReply(uint32_t from, uint64_t req_id, const std::string& payload);
    void HandleInstallSnapshot(uint32_t from, uint64_t req_id, const std::string& payload);
    void HandleInstallSnapshotReply(uint32_t from, uint64_t req_id, const std::string& payload);

    void BcastAppendEntries();
    void SendAppendEntriesTo(uint32_t peer_id, uint64_t req_id = 0);
    void SendInstallSnapshotTo(uint32_t peer_id, uint64_t req_id = 0);
    void CheckLeaderCommit();

    void EnqueueMessage(uint32_t to, raft_rpc::RaftOpcode opcode, uint64_t req_id, const std::string& payload);
    
    uint64_t GenerateReqId() { return next_req_id_++; }//reqid它是离散的、只要求全局唯一，不要求连续

private:
    uint32_t node_id_;                   // 本节点 ID
    std::vector<uint32_t> peer_ids_;      // 对端 Peer 节点 ID 列表
    std::vector<uint32_t> all_nodes_;     // 集群全量节点 ID 列表

    RaftRole role_{RaftRole::kFollower}; // 当前角色 (Follower / Candidate / Leader)
    uint32_t leader_id_{kNoLeader};      // 当前认定的 Leader 节点 ID

    HardState state_;                    // 包含 term, voted_for, commit_index, last_applied
    HardState prev_hard_state_;          // 上一次输出 Ready 时的 HardState 快照
    bool hard_state_dirty_{false};       // 显式 Dirty 标记，防止元数据未落盘

    RaftLog raft_log_;                   // 内存日志数组与水位线抽象

    std::unordered_set<uint32_t> voted_peers_; // Candidate 收集到的赞成票集合
    int election_elapsed_{0};             // 选举计时器流逝 tick 数
    int heartbeat_elapsed_{0};            // 心跳计时器流逝 tick 数
    int randomized_election_timeout_{0};  // 随机选举超时阈值

    std::unordered_map<uint32_t, uint64_t> next_index_;  // Leader 视角：准备发送给 Peer 的下一条日志 Index
    std::unordered_map<uint32_t, uint64_t> match_index_; // Leader 视角：Peer 已成功复制的最大日志 Index

    std::vector<OutboundMessage> pending_messages_; // 待投递的网络出站 RPC 队列
    std::mt19937 rng_;                             // 随机数生成引擎
    uint64_t next_req_id_{1};                      // 全局递增 req_id 发号器
};