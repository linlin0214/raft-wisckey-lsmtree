#pragma once

#include <vector>
#include <string>
#include <optional>
#include "protocol/RaftRpc.h"

enum class RaftRole {
    kFollower,
    KpreCandidate,
    kCandidate,
    kLeader
};

struct SnapshotMeta {
    uint64_t index{0};
    uint64_t term{0};
};

struct Snapshot {
    SnapshotMeta meta;
    bool is_valid{false};
    std::string data; // 若后续需要传 Memory 快照载荷
};

// Raft持久状态
struct HardState {
    uint64_t term{0};
    int32_t voted_for{-1};
    uint64_t commit_index{0};   // 补上 commit 水位
    uint64_t last_applied{0};   // 补上 apply 水位

    bool operator==(const HardState& other) const
    {
        return term == other.term &&
               voted_for == other.voted_for;
    }

    bool operator!=(const HardState& other) const
    {
        return !(*this == other);
    }
};

// 网络发送任务
struct OutboundMessage {
    uint32_t to_node_id{0};
    raft_rpc::RaftOpcode opcode;
    uint64_t req_id{0};
    std::string payload;
};

// Ready输出
struct Ready {
    // HardState变化
    std::optional<HardState> hard_state;
    // 新产生但是未稳定日志
    std::vector<raft_rpc::LogEntry> entries;
    // 已经commit等待Apply
    std::vector<raft_rpc::LogEntry> committed_entries;
    // RPC发送队列
    std::vector<OutboundMessage> messages;
    Snapshot snapshot; //  引用了 Snapshot 结构体
    bool IsEmpty() const
    {
        return !hard_state.has_value()
            && entries.empty()
            && committed_entries.empty()
            && messages.empty();
    }
};

// 外部完成反馈
struct AdvanceState {
    // WAL已经安全落盘到哪里
    uint64_t stabled_index{0};
    // 状态机已经Apply到哪里
    uint64_t applied_index{0};
};
