#include "RaftStorageAdapter.h"
#include "Config.h"
#include "Slice.h"
#include <spdlog/spdlog.h>
#include <filesystem>
#include <fstream>
#include <chrono>

RaftStorageAdapter::RaftStorageAdapter(const std::string& base_dir)
    : base_dir_(base_dir),
      log_engine_([&](){ std::filesystem::create_directories(base_dir); return base_dir + "/raft.log"; }()),
      state_machine_([&]() {
          Options opts;
          opts.disable_wal = true;
          return std::make_unique<DB>(base_dir + "/kv_data", opts);
      }()),
      meta_path_(base_dir + "/raft.meta"),
      snap_meta_path_(base_dir + "/snapshot.meta") {
    LoadHardState();
    LoadSnapshotMeta();
}

AdvanceState RaftStorageAdapter::PersistReady(const Ready& rd) {
    std::lock_guard<std::mutex> lock(storage_mtx_);

    uint64_t new_stabled_index = 0;
    uint64_t new_applied_index = last_applied_.load(std::memory_order_relaxed);

    if (!rd.entries.empty()) {
        uint64_t first_append_index = rd.entries.front().index;
        
        if (first_append_index <= log_engine_.GetLastIndex()) {
            log_engine_.TruncateSuffix(first_append_index - 1);
        }

        std::vector<RaftLogEngine::TaskEntry> batch;
        batch.reserve(rd.entries.size());
        size_t total_payload_bytes = 0;

        for (const auto& entry : rd.entries) {
            batch.push_back(RaftLogEngine::TaskEntry{entry.index, entry.term, entry.data});
            total_payload_bytes += entry.data.size();
        }

        auto start_time = std::chrono::high_resolution_clock::now();

        bool append_ok = log_engine_.AppendBatch(batch);
        bool sync_ok = log_engine_.SyncWAL();

        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - start_time).count();

        if (total_payload_bytes > 0 && (batch.size() >= 16 || elapsed_us > 2000)) {
            spdlog::info("[WAL Diagnostic] WAL Batch 刷盘监控 | batch_size: {} 条 | payload: {} B | sync_cost: {:.2f} ms",
                         batch.size(), total_payload_bytes, elapsed_us / 1000.0);
        }

        if (append_ok && sync_ok) {
            new_stabled_index = rd.entries.back().index;
        } else {
            spdlog::error("[RaftStorageAdapter] SyncWAL 物理刷盘失败，中断本轮持久化！");
            return AdvanceState{0, new_applied_index};
        }
    }

    if (!rd.committed_entries.empty()) {
        std::vector<std::pair<std::string, std::string>> apply_batch;
        apply_batch.reserve(rd.committed_entries.size());

        for (const auto& entry : rd.committed_entries) {
            if (entry.data.empty()) {
                new_applied_index = entry.index;
                continue;
            }

            auto parsed = raft_node::WireProtocol::Parse(entry.data);
            if (parsed.has_value()) {
                auto [header, key_view, val_view, skip] = *parsed;
                if (header.opcode == raft_node::Opcode::PUT_RAW || header.opcode == raft_node::Opcode::PUT_META) {
                    apply_batch.push_back({std::string(key_view), std::string(val_view)});
                } else if (header.opcode == raft_node::Opcode::DEL) {
                    apply_batch.push_back({std::string(key_view), config::TOMBSTONE});
                }
            }
            new_applied_index = entry.index;
        }

        if (!apply_batch.empty() && state_machine_) {
            state_machine_->PutBatch(apply_batch);
        }
        
        last_applied_.store(new_applied_index, std::memory_order_release);
        commit_index_.store(std::max(commit_index_.load(), new_applied_index), std::memory_order_release);
    }

    if (rd.hard_state.has_value()) {
        SaveHardState(rd.hard_state->term, rd.hard_state->voted_for, commit_index_.load(), last_applied_.load());
    }

    if (rd.snapshot.is_valid) {
        std::string snap_data_path = base_dir_ + "/snapshot.data";
        if (CreateSnapshot(snap_data_path, rd.snapshot.meta.index)) {
            SaveSnapshotMeta(rd.snapshot.meta.index, rd.snapshot.meta.term);
            log_engine_.TruncatePrefix(rd.snapshot.meta.index);
            spdlog::info("[RaftStorageAdapter] 快照落盘成功, index: {}, term: {}", 
                         rd.snapshot.meta.index, rd.snapshot.meta.term);
        } else {
            spdlog::error("[RaftStorageAdapter] 快照生成失败, index: {}", rd.snapshot.meta.index);
        }
    }

    return AdvanceState{new_stabled_index, new_applied_index};
}

bool RaftStorageAdapter::ApplyCommit(const std::string& raft_cmd) {
    if (!state_machine_) return false;

    if (raft_cmd.empty()) {
        return true;
    }

    auto parsed = raft_node::WireProtocol::Parse(raft_cmd);
    if (!parsed.has_value()) {
        return true;
    }

    auto [header, key_view, val_view, skip] = *parsed;

    switch (header.opcode) {
        case raft_node::Opcode::PUT_RAW:
        case raft_node::Opcode::PUT_META: {
            state_machine_->Put(Slice(key_view), std::string(val_view));
            return true;
        }
        case raft_node::Opcode::DEL: {
            state_machine_->Delete(Slice(key_view));
            return true;
        }
        default:
            return true;
    }
}

uint64_t RaftStorageAdapter::GetLogTerm(uint64_t index) {
    std::lock_guard<std::mutex> lock(storage_mtx_);
    uint64_t term = 0;
    std::string cmd;
    if (!log_engine_.Get(index, term, cmd)) {
        return 0;
    }
    return term;
}

bool RaftStorageAdapter::GetLog(uint64_t index, raft_rpc::LogEntry& out_entry) {
    std::lock_guard<std::mutex> lock(storage_mtx_);
    uint64_t term = 0;
    std::string cmd;
    if (!log_engine_.Get(index, term, cmd)) {
        return false;
    }
    out_entry.index = index;
    out_entry.term = term;
    out_entry.type = raft_rpc::EntryType::kNormal;
    out_entry.data = std::move(cmd);
    return true;
}

std::vector<raft_rpc::LogEntry> RaftStorageAdapter::GetLogEntries(uint64_t start_index, uint64_t end_index) {
    std::lock_guard<std::mutex> lock(storage_mtx_);
    std::vector<raft_rpc::LogEntry> result;
    for (uint64_t i = start_index; i <= end_index; ++i) {
        uint64_t term = 0;
        std::string data;
        if (log_engine_.Get(i, term, data)) {
            result.push_back(raft_rpc::LogEntry{i, term, raft_rpc::EntryType::kNormal, std::move(data)});
        } else {
            break;
        }
    }
    return result;
}

std::vector<raft_rpc::LogEntry> RaftStorageAdapter::LoadLogs() {
    std::lock_guard<std::mutex> lock(storage_mtx_);
    std::vector<raft_rpc::LogEntry> result;

    auto entries = log_engine_.LoadAllEntries();
    result.reserve(entries.size());

    for (auto& e : entries) {
        result.push_back(
            raft_rpc::LogEntry{
                e.index,
                e.term,
                raft_rpc::EntryType::kNormal,
                std::move(e.data)
            }
        );
    }

    return result;
}

HardState RaftStorageAdapter::GetHardState() const {
    std::lock_guard<std::mutex> lock(storage_mtx_);
    HardState hs;
    hs.term = current_term_.load(std::memory_order_acquire);
    hs.voted_for = voted_for_.load(std::memory_order_acquire);
    hs.commit_index = commit_index_.load(std::memory_order_acquire);
    hs.last_applied = last_applied_.load(std::memory_order_acquire);
    return hs;
}

void RaftStorageAdapter::TruncateLogConflict(uint64_t last_valid_index) {
    std::lock_guard<std::mutex> lock(storage_mtx_);
    log_engine_.TruncateSuffix(last_valid_index);
}

bool RaftStorageAdapter::SaveHardState(uint64_t term, int32_t voted_for, uint64_t commit_index, uint64_t applied_index) {
    std::lock_guard<std::mutex> lock(meta_mtx_);
    std::string tmp_path = meta_path_ + ".tmp";
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;

    std::string meta_str = std::to_string(term) + " " + 
                           std::to_string(voted_for) + " " + 
                           std::to_string(commit_index) + " " + 
                           std::to_string(applied_index);

    if (::write(fd, meta_str.data(), meta_str.size()) != static_cast<ssize_t>(meta_str.size())) {
        ::close(fd);
        return false;
    }

    if (::fdatasync(fd) != 0) {
        ::close(fd);
        return false;
    }
    ::close(fd);

    if (::rename(tmp_path.c_str(), meta_path_.c_str()) != 0) {
        return false;
    }

    current_term_.store(term, std::memory_order_release);
    voted_for_.store(voted_for, std::memory_order_release);
    commit_index_.store(commit_index, std::memory_order_release);
    last_applied_.store(applied_index, std::memory_order_release);
    return true;
}

void RaftStorageAdapter::LoadHardState() {
    std::lock_guard<std::mutex> lock(meta_mtx_);
    
    current_term_.store(0, std::memory_order_relaxed);
    voted_for_.store(-1, std::memory_order_relaxed);
    commit_index_.store(0, std::memory_order_relaxed);
    last_applied_.store(0, std::memory_order_relaxed);

    std::ifstream ifs(meta_path_);
    if (ifs.is_open()) {
        uint64_t t = 0, c = 0, a = 0; 
        int32_t v = -1;
        if (ifs >> t >> v) {
            current_term_.store(t, std::memory_order_relaxed);
            voted_for_.store(v, std::memory_order_relaxed);
            if (ifs >> c >> a) {
                commit_index_.store(c, std::memory_order_relaxed);
                last_applied_.store(a, std::memory_order_relaxed);
            }
        }
    }
}

bool RaftStorageAdapter::SaveSnapshotMeta(uint64_t index, uint64_t term) {
    std::lock_guard<std::mutex> lock(meta_mtx_);
    std::string tmp_path = snap_meta_path_ + ".tmp";
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;

    std::string str = std::to_string(index) + " " + std::to_string(term);
    if (::write(fd, str.data(), str.size()) != static_cast<ssize_t>(str.size())) {
        ::close(fd);
        return false;
    }

    ::fdatasync(fd);
    ::close(fd);
    ::rename(tmp_path.c_str(), snap_meta_path_.c_str());

    snap_last_index_.store(index, std::memory_order_release);
    snap_last_term_.store(term, std::memory_order_release);
    return true;
}

void RaftStorageAdapter::LoadSnapshotMeta() {
    std::lock_guard<std::mutex> lock(meta_mtx_);
    std::ifstream ifs(snap_meta_path_);
    if (ifs.is_open()) {
        uint64_t idx = 0, term = 0;
        if (ifs >> idx >> term) {
            snap_last_index_.store(idx, std::memory_order_relaxed);
            snap_last_term_.store(term, std::memory_order_relaxed);
        }
    }
}

Snapshot RaftStorageAdapter::GetSnapshot() const {
    Snapshot snap;
    snap.meta.index = snap_last_index_.load(std::memory_order_acquire);
    snap.meta.term = snap_last_term_.load(std::memory_order_acquire);
    snap.is_valid = (snap.meta.index > 0);
    return snap;
}

std::string RaftStorageAdapter::StateMachineGet(const Slice& key) {
    std::lock_guard<std::mutex> lock(storage_mtx_);
    return state_machine_ ? state_machine_->Get(key) : "NOT_FOUND";
}

bool RaftStorageAdapter::CreateSnapshot(const std::string& snapshot_file_path, uint64_t snapshot_index) {
    if (!state_machine_) return false;

    std::string tmp_path = snapshot_file_path + ".tmp";
    std::ofstream ofs(tmp_path, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) return false;

    auto merge_iter = state_machine_->NewMergingIterator();

    while (merge_iter->Valid()) {
        Slice key = merge_iter->Key();
        std::string raw_ptr_or_val = merge_iter->Value();

        std::string actual_val;
        if (raw_ptr_or_val.size() == sizeof(VLogPointer)) {
            VLogPointer ptr = VLogPointer::Decode(raw_ptr_or_val);
            actual_val = (ptr.size > 0) ? state_machine_->ReadVLog(ptr) : "";
        } else {
            actual_val = raw_ptr_or_val;
        }

        uint16_t klen = static_cast<uint16_t>(key.size());
        uint32_t vlen = static_cast<uint32_t>(actual_val.size());
        ofs.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
        ofs.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
        if (klen > 0) ofs.write(key.data(), klen);
        if (vlen > 0) ofs.write(actual_val.data(), vlen);

        merge_iter->Next();
    }

    ofs.flush();
    int fd = ::open(tmp_path.c_str(), O_RDONLY);
    if (fd >= 0) { 
        ::fsync(fd); 
        ::close(fd); 
    }
    ::rename(tmp_path.c_str(), snapshot_file_path.c_str());
    return true; 
}

void RaftStorageAdapter::ApplySnapshot(const std::string& snapshot_file_path, uint64_t last_included_index) {
    std::lock_guard<std::mutex> lock(storage_mtx_);

    std::string target_dir = base_dir_for_vlog_();
    std::string tmp_dir = target_dir + "_tmp";
    std::string backup_dir = target_dir + "_old";

    state_machine_.reset(); 
    DB::DestroyDB(tmp_dir); 
    
    Options opts;
    opts.disable_wal = true;

    auto tmp_db = std::make_unique<DB>(tmp_dir);
    std::ifstream ifs(snapshot_file_path, std::ios::binary);
    uint16_t klen;
    uint32_t vlen;
    
    while (ifs.read(reinterpret_cast<char*>(&klen), sizeof(klen))) {
        ifs.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
        std::string key(klen, '\0');
        if (klen > 0) ifs.read(&key[0], klen);
        std::string val(vlen, '\0');
        if (vlen > 0) ifs.read(&val[0], vlen);
        tmp_db->Put(Slice(key), val);
    }
    
    tmp_db.reset();

    std::error_code ec;
    std::filesystem::remove_all(backup_dir, ec);

    if (std::filesystem::exists(target_dir)) {
        std::filesystem::rename(target_dir, backup_dir, ec);
    }
    std::filesystem::rename(tmp_dir, target_dir, ec);

    int dir_fd = ::open(base_dir_.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd >= 0) {
        ::fsync(dir_fd);
        ::close(dir_fd);
    }

    std::filesystem::remove_all(backup_dir, ec);
    state_machine_ = std::make_unique<DB>(target_dir, opts);
    log_engine_.TruncatePrefix(last_included_index);
    SaveHardState(current_term_.load(), voted_for_.load(), last_included_index, last_included_index);
}