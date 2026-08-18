#pragma once

#include "RaftLogEngine.h"
#include "DB.h"
#include "raft/RaftTypes.h"
#include "protocol/RaftRpc.h"
#include "protocol/wire_protocol.h"
#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <memory>
#include <atomic>
#include <vector>
#include <mutex>

class RaftStorageAdapter {
public:
    explicit RaftStorageAdapter(const std::string& base_dir);
    ~RaftStorageAdapter() = default;

    // 1. 核心 Pipeline：一站式消费 Ready 物理副作用，并返回 Advance 反馈进度
    AdvanceState PersistReady(const Ready& rd);

    // 2. RaftCore / Leader 复制所需的日志查询接口
    uint64_t GetLogTerm(uint64_t index);
    bool GetLog(uint64_t index, raft_rpc::LogEntry& out_entry);
    std::vector<raft_rpc::LogEntry> GetLogEntries(uint64_t start_index, uint64_t end_index);
    
    // 冷启动恢复：加载 WAL 中全部日志
    std::vector<raft_rpc::LogEntry> LoadLogs();

    // 3. 元数据与水位线存取 (包含 term, voted_for, commit_index, applied_index)
    bool SaveHardState(uint64_t term, int32_t voted_for, uint64_t commit_index, uint64_t applied_index);
    
    // 冷启动恢复：获取持久化的 4 元组 HardState 快照
    HardState GetHardState() const;

    uint64_t GetCurrentTerm() const { return current_term_.load(std::memory_order_acquire); }
    int32_t GetVotedFor() const { return voted_for_.load(std::memory_order_acquire); }
    uint64_t GetCommitIndex() const { return commit_index_.load(std::memory_order_acquire); }
    uint64_t GetLastApplied() const { return last_applied_.load(std::memory_order_acquire); }
    uint64_t GetLastLogIndex() const { return log_engine_.GetLastIndex(); }
    uint64_t GetFirstLogIndex() const { return log_engine_.GetFirstIndex(); }

    // 4. 状态机底层操作接口
    bool ApplyCommit(const std::string& raft_cmd);
    std::string StateMachineGet(int key);

    // 🚀 5. 快照与持久化元数据存取
    bool CreateSnapshot(const std::string& snapshot_file_path, uint64_t snapshot_index);
    void ApplySnapshot(const std::string& snapshot_file_path, uint64_t last_included_index);
    
    // 冷启动读取 Snapshot 元数据
    Snapshot GetSnapshot() const;
    bool SaveSnapshotMeta(uint64_t index, uint64_t term);

private:
    void LoadHardState();
    void LoadSnapshotMeta();
    void TruncateLogConflict(uint64_t last_valid_index);
    std::string base_dir_for_vlog_() const { return base_dir_ + "/kv_data"; }

private:
    std::string base_dir_;
    RaftLogEngine log_engine_;
    std::unique_ptr<DB> state_machine_; 
    std::string meta_path_;
    std::string snap_meta_path_; // 🚀 快照元数据路径
    
    // 完整的 Raft 持久化 4 元组状态
    std::atomic<uint64_t> current_term_{0};
    std::atomic<int32_t> voted_for_{-1};
    std::atomic<uint64_t> commit_index_{0};
    std::atomic<uint64_t> last_applied_{0};

    // 快照元数据内存缓存
    std::atomic<uint64_t> snap_last_index_{0};
    std::atomic<uint64_t> snap_last_term_{0};

    // 核心隔离锁：确保整个 Storage 管线串行化，彻底消除并发竞态
    mutable std::mutex storage_mtx_;
    mutable std::mutex meta_mtx_;
};