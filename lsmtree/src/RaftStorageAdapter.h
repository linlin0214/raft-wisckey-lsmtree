#pragma once

#include "RaftLogEngine.h"
#include "DB.h"
#include "raft/RaftTypes.h"
#include "protocol/RaftRpc.h"
#include "protocol/wire_protocol.h"
#include "Slice.h"
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

    AdvanceState PersistReady(const Ready& rd);

    uint64_t GetLogTerm(uint64_t index);
    bool GetLog(uint64_t index, raft_rpc::LogEntry& out_entry);
    std::vector<raft_rpc::LogEntry> GetLogEntries(uint64_t start_index, uint64_t end_index);
    
    std::vector<raft_rpc::LogEntry> LoadLogs();

    bool SaveHardState(uint64_t term, int32_t voted_for, uint64_t commit_index, uint64_t applied_index);
    HardState GetHardState() const;

    uint64_t GetCurrentTerm() const { return current_term_.load(std::memory_order_acquire); }
    int32_t GetVotedFor() const { return voted_for_.load(std::memory_order_acquire); }
    uint64_t GetCommitIndex() const { return commit_index_.load(std::memory_order_acquire); }
    uint64_t GetLastApplied() const { return last_applied_.load(std::memory_order_acquire); }
    uint64_t GetLastLogIndex() const { return log_engine_.GetLastIndex(); }
    uint64_t GetFirstLogIndex() const { return log_engine_.GetFirstIndex(); }

    bool ApplyCommit(const std::string& raft_cmd);
    std::string StateMachineGet(const Slice& key);

    bool CreateSnapshot(const std::string& snapshot_file_path, uint64_t snapshot_index);
    void ApplySnapshot(const std::string& snapshot_file_path, uint64_t last_included_index);
    
    Snapshot GetSnapshot() const;
    bool SaveSnapshotMeta(uint64_t index, uint64_t term);

    void ForceSync() {
        std::lock_guard<std::mutex> lock(storage_mtx_);
        log_engine_.SyncWAL();
        if (state_machine_) {
            state_machine_->Sync();
        }
    }

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
    std::string snap_meta_path_;
    
    std::atomic<uint64_t> current_term_{0};
    std::atomic<int32_t> voted_for_{-1};
    std::atomic<uint64_t> commit_index_{0};
    std::atomic<uint64_t> last_applied_{0};

    std::atomic<uint64_t> snap_last_index_{0};
    std::atomic<uint64_t> snap_last_term_{0};

    mutable std::mutex storage_mtx_;
    mutable std::mutex meta_mtx_;
};