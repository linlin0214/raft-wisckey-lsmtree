#pragma once

#include <vector>
#include <cstdint>
#include <algorithm>
#include "protocol/RaftRpc.h"

class RaftLog {
public:
    RaftLog();
    ~RaftLog() = default;

    // 冷启动与状态恢复 (自动将 stabled_index_ 对齐至 WAL 恢复出的 GetLastIndex())
    void Restore(
        uint64_t snapshot_index,
        uint64_t snapshot_term,
        std::vector<raft_rpc::LogEntry> entries,
        uint64_t commit_index,
        uint64_t last_applied
    );

    // 核心信息查询
    uint64_t GetLastIndex() const;
    uint64_t GetLastTerm() const;
    uint64_t GetTerm(uint64_t index) const;

    // 水位线获取与修改
    uint64_t stabled_index() const { return stabled_index_; }
    uint64_t commit_index() const { return commit_index_; }
    uint64_t last_applied() const { return last_applied_; }
    uint64_t last_included_index() const { return last_included_index_; }
    uint64_t last_included_term() const { return last_included_term_; }

    void set_commit_index(uint64_t idx) { commit_index_ = std::max(commit_index_, idx); }

    // Ready 物理副作用提取接口 ( 流量控制：限制单次 Committed 最多 64 条 / 64KB，防止深拷贝卡死 EventLoop)
    std::vector<raft_rpc::LogEntry> UnstableEntries() const;
    std::vector<raft_rpc::LogEntry> CommittedUnappliedEntries(
    size_t max_entries = 256,         //   256 条
    size_t max_bytes = 1024 * 1024    //   1MB
) const;

    // Advance 水位线单调推进
    void StableTo(uint64_t index);
    void AppliedTo(uint64_t index);

    // 日志读取与追加 ( 流量控制：限制单次 RPC 最多 64 条 / 64KB，防止大包传输)
    const raft_rpc::LogEntry* GetEntry(uint64_t index) const;
    std::vector<raft_rpc::LogEntry> GetEntries(
    uint64_t start_index, 
    size_t max_bytes = 1024 * 1024,   //  1MB (可容纳 ~256 条 4KB 日志)
    size_t max_entries = 256          //  256 条
) const;

    bool Append(const raft_rpc::LogEntry& entry);
    bool Append(raft_rpc::LogEntry&& entry);
    
    //  支持右值移动语义追加，彻底消除 Follower 追加日志时的 std::string 深拷贝
    void Append(const std::vector<raft_rpc::LogEntry>& entries);
    void Append(std::vector<raft_rpc::LogEntry>&& entries);

    // 日志截断与快照压缩
    void TruncateSuffix(uint64_t index); 
    void TruncatePrefix(uint64_t snapshot_index, uint64_t snapshot_term); 

    bool CanCompact(uint64_t index) const;

private:
    size_t ToVectorIndex(uint64_t index) const;

private:
    std::vector<raft_rpc::LogEntry> entries_; // 内存日志逻辑数组

    uint64_t last_included_index_{0}; // 快照覆盖的最大逻辑 Index
    uint64_t last_included_term_{0};  // 快照覆盖的最大 Term

    uint64_t stabled_index_{0};   // WAL 刷盘进度
    uint64_t commit_index_{0};    // 共识进度
    uint64_t last_applied_{0};    // 状态机 Apply 进度
};