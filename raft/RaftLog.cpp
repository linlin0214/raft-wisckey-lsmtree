#include "RaftLog.h"

RaftLog::RaftLog() {
    // 构造函数初始化 Sentinel 节点 (index = 0, term = 0)
    entries_.push_back(raft_rpc::LogEntry{0, 0, raft_rpc::EntryType::kNormal, ""});
}

void RaftLog::Restore(uint64_t snapshot_index,
                      uint64_t snapshot_term,
                      std::vector<raft_rpc::LogEntry> entries,
                      uint64_t commit_index,
                      uint64_t last_applied) {
    last_included_index_ = snapshot_index;
    last_included_term_ = snapshot_term;

    entries_.clear();
    // 1. 注入 Sentinel 节点
    entries_.push_back(raft_rpc::LogEntry{
        last_included_index_,
        last_included_term_,
        raft_rpc::EntryType::kNormal,
        ""
    });

    // 2. 物理恢复磁盘 WAL 中超越快照边界的日志
    for (auto& entry : entries) {
        if (entry.index > last_included_index_) {
            entries_.push_back(std::move(entry));
        }
    }

    //将从磁盘 WAL 中恢复出来的这些日志，直接标记为Stable
    stabled_index_ = GetLastIndex();

    // 3. 水位线恢复并防止倒退
    commit_index_ = std::max(commit_index, last_included_index_);
    last_applied_ = std::max(last_applied, last_included_index_);
}

size_t RaftLog::ToVectorIndex(uint64_t index) const {
    return static_cast<size_t>(index - last_included_index_);
}

uint64_t RaftLog::GetLastIndex() const {
    return entries_.back().index;
}

uint64_t RaftLog::GetLastTerm() const {
    return entries_.back().term;
}

uint64_t RaftLog::GetTerm(uint64_t index) const {
    if (index == last_included_index_) {
        return last_included_term_;
    }
    if (index < last_included_index_ || index > GetLastIndex()) {
        return 0;
    }
    return entries_[ToVectorIndex(index)].term;
}

const raft_rpc::LogEntry* RaftLog::GetEntry(uint64_t index) const {
    if (index < last_included_index_ || index > GetLastIndex()) {
        return nullptr;
    }
    return &entries_[ToVectorIndex(index)];
}

//  按包体积 max_bytes (默认64KB) 与条数 max_entries (默认64条) 双重截断控包，安全且高效地批量获取日志
std::vector<raft_rpc::LogEntry> RaftLog::GetEntries(uint64_t start_index, size_t max_bytes, size_t max_entries) const {
    std::vector<raft_rpc::LogEntry> res;
    
    if (start_index <= last_included_index_) {
        start_index = last_included_index_ + 1;
    }

    if (start_index > GetLastIndex()) {
        return res;
    }

    size_t start_vec_idx = ToVectorIndex(start_index);
    size_t current_bytes = 0;

    for (size_t i = start_vec_idx; i < entries_.size(); ++i) {
        res.push_back(entries_[i]);
        current_bytes += entries_[i].data.size();
        if (current_bytes >= max_bytes || res.size() >= max_entries) break;
    }
    return res;
}

bool RaftLog::Append(const raft_rpc::LogEntry& entry) {
    if (entry.index != GetLastIndex() + 1) {
        return false;
    }
    entries_.push_back(entry);
    return true;
}

//右值通常代表“临时的、马上就要被销毁的、我不再需要的”对象
bool RaftLog::Append(raft_rpc::LogEntry&& entry) {
    if (entry.index != GetLastIndex() + 1) {
        return false;
    }
    entries_.push_back(std::move(entry));
    return true;
}

void RaftLog::Append(const std::vector<raft_rpc::LogEntry>& new_entries){
    if(new_entries.empty())
        return;

    size_t offset = 0;

    for(; offset < new_entries.size(); ++offset)
    {
        uint64_t index = new_entries[offset].index;
        auto local = GetEntry(index);

        if(local == nullptr)
        {
            break;
        }
        if(local->term != new_entries[offset].term)
        {
            TruncateSuffix(index);
            break;
        }
    }
    for(; offset < new_entries.size(); ++offset)
    {
        if(new_entries[offset].index != GetLastIndex()+1)
            break;
        entries_.push_back(new_entries[offset]);
    }
}

//  右值移动重载：使用 std::move，构建临时对象，消灭 Follower 追加时的堆内存分配与深拷贝
void RaftLog::Append(std::vector<raft_rpc::LogEntry>&& new_entries) {
    if (new_entries.empty()) return;

    size_t offset = 0;
    for (; offset < new_entries.size(); ++offset) {
        uint64_t index = new_entries[offset].index;
        auto local = GetEntry(index);

        if (local == nullptr) break;
        if (local->term != new_entries[offset].term) {
            TruncateSuffix(index);
            break;
        }
    }
    for (; offset < new_entries.size(); ++offset) {
        if (new_entries[offset].index != GetLastIndex() + 1) break;
        entries_.push_back(std::move(new_entries[offset]));
    }
}

void RaftLog::TruncateSuffix(uint64_t index) {
    if (index <= last_included_index_) {
        return;
    }

    if (index > GetLastIndex()) {
        return;
    }

    if (index <= commit_index_) {
        return;
    }

    size_t pos = ToVectorIndex(index);

    entries_.erase(
        entries_.begin() + pos,
        entries_.end()
    );

    if (stabled_index_ >= index) {
        stabled_index_ = index - 1;
    }
}
//当节点在本地生成并应用了一个新快照后，需要把内存中快照之前的旧日志物理截断释放掉
void RaftLog::TruncatePrefix(uint64_t snapshot_index, uint64_t snapshot_term) {
    if (snapshot_index <= last_included_index_) {
        return;
    }

    std::vector<raft_rpc::LogEntry> new_entries;
    new_entries.push_back(raft_rpc::LogEntry{
        snapshot_index,
        snapshot_term,
        raft_rpc::EntryType::kNormal,
        ""
    });

    if (snapshot_index < GetLastIndex()) {
        for (auto& e : entries_) {
            if (e.index > snapshot_index) {
                new_entries.push_back(std::move(e));
            }
        }
    }

    entries_ = std::move(new_entries);

    last_included_index_ = snapshot_index;
    last_included_term_ = snapshot_term;

    stabled_index_ = std::max(stabled_index_, snapshot_index);
    commit_index_ = std::max(commit_index_, snapshot_index);
    last_applied_ = std::max(last_applied_, snapshot_index);
}

bool RaftLog::CanCompact(uint64_t index) const {
    return index <= last_applied_ && index > last_included_index_;
}

//Leader 在将日志追加到本地后，需要将其持久化到磁盘（WAL）之前
std::vector<raft_rpc::LogEntry> RaftLog::UnstableEntries() const {
    std::vector<raft_rpc::LogEntry> result;
    uint64_t start_idx = stabled_index_ + 1;
    if (start_idx <= last_included_index_) {
        start_idx = last_included_index_ + 1;
    }

    if (start_idx > GetLastIndex()) {
        return result;
    }

    size_t start_vec_idx = ToVectorIndex(start_idx);
    for (size_t i = start_vec_idx; i < entries_.size(); ++i) {
        result.push_back(entries_[i]);
    }
    return result;
}

//  Apply 流量控制：限制单次最多 64 条 / 64KB，防止状态机 Apply 拖挂 EventLoop，日志被多数派确认后，需要应用到上层的状态机 KV 数据库中
std::vector<raft_rpc::LogEntry> RaftLog::CommittedUnappliedEntries(size_t max_entries, size_t max_bytes) const {
    std::vector<raft_rpc::LogEntry> result;
    uint64_t start_idx = last_applied_ + 1;
    if (start_idx <= last_included_index_) {
        start_idx = last_included_index_ + 1;
    }

    uint64_t end_idx = std::min(commit_index_, GetLastIndex());
    if (start_idx > end_idx) {
        return result;
    }

    size_t start_vec = ToVectorIndex(start_idx);
    size_t end_vec = ToVectorIndex(end_idx);
    size_t current_bytes = 0;

    for (size_t i = start_vec; i <= end_vec && i < entries_.size(); ++i) {
        current_bytes += entries_[i].data.size();
        result.push_back(entries_[i]);
        
        // 只要已经拿到了至少 1 条日志，且达到了字节数或条数限制，才退出循环
        if (result.size() >= max_entries || current_bytes >= max_bytes) {
            break;
        }
    }
    return result;
}

//异步 I/O 线程完成了磁盘写入，回调通知 Raft 核心线程时
void RaftLog::StableTo(uint64_t index) {
    stabled_index_ = std::max(stabled_index_, index);
}

void RaftLog::AppliedTo(uint64_t index) {
    uint64_t safe_applied = std::min(index, commit_index_);
    last_applied_ = std::max(last_applied_, safe_applied);
}