#pragma once

#include <unordered_set>
#include <vector>
#include <string>
#include <optional>
#include <cstdint>
#include "protocol/RaftRpc.h"

//  1. 基础类型别名与全局哨兵常量
using NodeId = uint32_t;
inline constexpr NodeId kNoLeader = 0;
inline constexpr int32_t kNoVote = -1;

//  2. Raft 角色枚举（规范命名）
enum class RaftRole : uint8_t {
    kFollower,
    kPreCandidate, // 预候选人（Pre-Vote 试探阶段）
    kCandidate,    // 正式候选人（已递增 Term 并发起决选）
    kLeader
};

//  3. 快照元数据与快照载荷
struct SnapshotMeta {
    uint64_t index{0};
    uint64_t term{0};
};

struct Snapshot {
    SnapshotMeta meta;
    bool is_valid{false};
    std::string data; // 内存快照数据或物理快照路径标识

    bool IsValid() const { return is_valid && meta.index > 0; }
};

//  4. Raft 核心持久化 4 元组 HardState
struct HardState {
    uint64_t term{0};
    int32_t voted_for{kNoVote};
    uint64_t commit_index{0};
    uint64_t last_applied{0};

    bool operator==(const HardState& other) const {
        return term == other.term &&
               voted_for == other.voted_for &&
               commit_index == other.commit_index &&
               last_applied == other.last_applied;
    }

    bool operator!=(const HardState& other) const {
        return !(*this == other);
    }
};

//  5. 线性一致性读状态 (ReadIndex 机制)
struct ReadState {
    uint64_t read_index{0};    // 当前已提交的安全读取水位
    std::string request_ctx;   // 客户端请求唯一上下文 (Correlation ID)
};

//  6. 集群成员动态变更 (Membership Change)
enum class ConfChangeType : uint8_t {
    kAddNode = 0,
    kRemoveNode = 1
};

struct ConfChange {
    uint64_t change_id{0};
    ConfChangeType type{ConfChangeType::kAddNode};
    NodeId node_id{0};
    std::string context;
};

//  7. 外部网络发送任务
struct OutboundMessage {
    NodeId to_node_id{0};
    raft_rpc::RaftOpcode opcode;
    uint64_t req_id{0};
    std::string payload;
};

//  8. Ready 批处理输出结构（修复 IsEmpty 漏洞）
struct Ready {
    // 发生物理变更的 HardState
    std::optional<HardState> hard_state;
    // 待写入 WAL 的新日志条目
    std::vector<raft_rpc::LogEntry> entries;
    // 已达成共识待 Apply 到状态机的条目
    std::vector<raft_rpc::LogEntry> committed_entries;
    // 待通过网络广播的 RPC 任务队列
    std::vector<OutboundMessage> messages;
    // 待应用的快照
    Snapshot snapshot;
    // 待向客户端返回的安全 Read 水位
    std::vector<ReadState> read_states;

    bool IsEmpty() const {
        return !hard_state.has_value()
            && entries.empty()
            && committed_entries.empty()
            && messages.empty()
            && !snapshot.is_valid
            && read_states.empty();
    }
};

// 9. 外部物理执行进度反馈
struct AdvanceState {
    uint64_t stabled_index{0}; // WAL 已安全落盘的物理位点
    uint64_t applied_index{0}; // 状态机已成功 Apply 的业务位点
};

struct ReadIndexRequest {
        std::string ctx;
        uint64_t read_index;
        std::unordered_set<uint32_t> acks; // 收集心跳确认的节点集合
    };