#pragma once

#include <string>
#include <vector>
#include <map>
#include <cstdint>
#include <mutex>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdexcept>
#include <algorithm>
#include <spdlog/spdlog.h>

class RaftLogEngine {
public:
    struct TaskEntry {
        uint64_t index;
        uint64_t term;
        std::string data;
    };

    struct EntryHeader {
        uint64_t index;
        uint64_t term;
        uint32_t val_size;
    };

    explicit RaftLogEngine(const std::string& log_path) : log_path_(log_path) {
        active_fd_ = ::open(log_path_.c_str(), O_RDWR | O_CREAT, 0644);
        if (active_fd_ < 0) {
            throw std::runtime_error("Failed to open WAL log file: " + log_path_);
        }
        RecoverAndInit();
    }

    ~RaftLogEngine() {
        if (active_fd_ >= 0) {
            ::fdatasync(active_fd_);
            ::close(active_fd_);
            active_fd_ = -1;
        }
    }

    bool AppendBatch(const std::vector<TaskEntry>& batch) {
        if (batch.empty()) return true;

        std::lock_guard<std::mutex> lock(mtx_);

        uint64_t old_last_index = last_index_;
        uint32_t old_last_val_size = last_entry_val_size_;
        off_t start_file_offset = current_file_size_;

        size_t total_bytes = 0;
        for (const auto& task : batch) {
            total_bytes += sizeof(EntryHeader) + task.data.size();
        }

        EnsurePreallocatedSpace(current_file_size_ + total_bytes);

        std::string write_buf;
        write_buf.reserve(total_bytes);

        std::vector<std::pair<uint64_t, off_t>> temp_offsets;
        temp_offsets.reserve(batch.size());

        off_t curr_off = current_file_size_;

        for (const auto& task : batch) {
            EntryHeader hdr{task.index, task.term, static_cast<uint32_t>(task.data.size())};
            write_buf.append(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
            if (!task.data.empty()) {
                write_buf.append(task.data.data(), task.data.size());
            }
            temp_offsets.push_back({task.index, curr_off});
            curr_off += sizeof(hdr) + task.data.size();
        }

        ssize_t written = ::pwrite(active_fd_, write_buf.data(), write_buf.size(), start_file_offset);
        if (written != static_cast<ssize_t>(write_buf.size())) {
            last_index_ = old_last_index;
            last_entry_val_size_ = old_last_val_size;
            ::ftruncate(active_fd_, start_file_offset);
            current_file_size_ = start_file_offset;
            allocated_size_ = start_file_offset;
            return false;
        }

        for (const auto& pair : temp_offsets) {
            index_to_offset_[pair.first] = pair.second;
        }

        if (first_index_ == 0) first_index_ = batch.front().index;
        last_index_ = batch.back().index;
        last_entry_val_size_ = static_cast<uint32_t>(batch.back().data.size());
        current_file_size_ += written;
        return true;
    }

    bool SyncWAL() {
        std::lock_guard<std::mutex> lock(mtx_);
        if (active_fd_ < 0) return false;
        return ::fdatasync(active_fd_) == 0;
    }

    bool Get(uint64_t index, uint64_t& out_term, std::string& out_cmd) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = index_to_offset_.find(index);
        if (it == index_to_offset_.end()) return false;

        off_t offset = it->second;
        EntryHeader hdr;
        if (::pread(active_fd_, &hdr, sizeof(hdr), offset) != sizeof(hdr)) return false;

        out_term = hdr.term;
        out_cmd.resize(hdr.val_size);
        if (hdr.val_size > 0) {
            if (::pread(active_fd_, &out_cmd[0], hdr.val_size, offset + sizeof(hdr)) != static_cast<ssize_t>(hdr.val_size)) {
                return false;
            }
        }
        return true;
    }

    void TruncateSuffix(uint64_t target_index) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (target_index >= last_index_) return;

        auto it = index_to_offset_.upper_bound(target_index);
        if (it != index_to_offset_.end()) {
            off_t truncate_offset = it->second;
            ::ftruncate(active_fd_, truncate_offset);
            current_file_size_ = truncate_offset;
            allocated_size_ = truncate_offset;
            index_to_offset_.erase(it, index_to_offset_.end());
        }

        last_index_ = target_index;

        if (index_to_offset_.count(target_index)) {
            off_t last_off = index_to_offset_[target_index];
            EntryHeader hdr;
            if (::pread(active_fd_, &hdr, sizeof(hdr), last_off) == sizeof(hdr)) {
                last_entry_val_size_ = hdr.val_size;
            }
        }
    }

    // 🚀 核心物理压缩：截断已 Snapshot 的前缀历史日志，回收 WAL 物理空间
    void TruncatePrefix(uint64_t snapshot_index) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (snapshot_index < first_index_) return;

        // 1. 如果包含全部日志，直接原子清空 WAL 物理文件
        if (snapshot_index >= last_index_) {
            ::ftruncate(active_fd_, 0);
            current_file_size_ = 0;
            allocated_size_ = 0;
            index_to_offset_.clear();
            first_index_ = snapshot_index + 1;
            last_index_ = snapshot_index;
            return;
        }

        // 2. 若有保留日志，将 snapshot_index 之后的有效日志存入新文件，原子替换原 WAL
        std::string tmp_log_path = log_path_ + ".compact.tmp";
        int tmp_fd = ::open(tmp_log_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (tmp_fd < 0) return;

        auto it_start = index_to_offset_.lower_bound(snapshot_index + 1);
        std::map<uint64_t, off_t> new_index_map;
        off_t new_file_size = 0;

        for (auto it = it_start; it != index_to_offset_.end(); ++it) {
            off_t old_off = it->second;
            EntryHeader hdr;
            if (::pread(active_fd_, &hdr, sizeof(hdr), old_off) != sizeof(hdr)) break;

            std::string data(hdr.val_size, '\0');
            if (hdr.val_size > 0) {
                if (::pread(active_fd_, &data[0], hdr.val_size, old_off + sizeof(hdr)) != static_cast<ssize_t>(hdr.val_size)) break;
            }

            // 追加写入临时日志文件
            ::pwrite(tmp_fd, &hdr, sizeof(hdr), new_file_size);
            if (hdr.val_size > 0) {
                ::pwrite(tmp_fd, data.data(), hdr.val_size, new_file_size + sizeof(hdr));
            }

            new_index_map[hdr.index] = new_file_size;
            new_file_size += sizeof(hdr) + hdr.val_size;
        }

        ::fdatasync(tmp_fd);
        ::close(tmp_fd);

        // 关闭老文件描述符，原位替换
        ::close(active_fd_);
        ::rename(tmp_log_path.c_str(), log_path_.c_str());

        // 重新打开生效
        active_fd_ = ::open(log_path_.c_str(), O_RDWR, 0644);
        current_file_size_ = new_file_size;
        allocated_size_ = new_file_size;
        index_to_offset_ = std::move(new_index_map);
        first_index_ = snapshot_index + 1;
    }

    std::vector<TaskEntry> LoadAllEntries() {
        std::lock_guard<std::mutex> lock(mtx_);
        std::vector<TaskEntry> res;
        off_t curr_off = 0;
        struct stat st;
        if (::fstat(active_fd_, &st) != 0) return res;

        while (curr_off < st.st_size) {
            EntryHeader hdr;
            if (::pread(active_fd_, &hdr, sizeof(hdr), curr_off) != sizeof(hdr)) break;

            std::string data(hdr.val_size, '\0');
            if (hdr.val_size > 0) {
                if (::pread(active_fd_, &data[0], hdr.val_size, curr_off + sizeof(hdr)) != static_cast<ssize_t>(hdr.val_size)) break;
            }

            res.push_back(TaskEntry{hdr.index, hdr.term, std::move(data)});
            curr_off += sizeof(hdr) + hdr.val_size;
        }
        return res;
    }

    uint64_t GetLastIndex() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return last_index_;
    }

    uint64_t GetFirstIndex() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return first_index_;
    }

private:
    void EnsurePreallocatedSpace(off_t required_bytes) {
        if (required_bytes > allocated_size_) {
            off_t new_alloc = std::max(allocated_size_ + 64 * 1024 * 1024, required_bytes);
            if (active_fd_ >= 0) {
                ::posix_fallocate(active_fd_, 0, new_alloc);
            }
            allocated_size_ = new_alloc;
        }
    }

    void RecoverAndInit() {
        struct stat st;
        if (::fstat(active_fd_, &st) != 0) return;
        current_file_size_ = st.st_size;
        allocated_size_ = st.st_size;

        off_t curr_off = 0;
        while (curr_off < current_file_size_) {
            EntryHeader hdr;
            if (::pread(active_fd_, &hdr, sizeof(hdr), curr_off) != sizeof(hdr)) break;

            if (first_index_ == 0) first_index_ = hdr.index;
            index_to_offset_[hdr.index] = curr_off;
            last_index_ = hdr.index;
            last_entry_val_size_ = hdr.val_size;

            curr_off += sizeof(hdr) + hdr.val_size;
        }
    }

private:
    std::string log_path_;
    int active_fd_{-1};
    uint64_t first_index_{0};
    uint64_t last_index_{0};
    uint32_t last_entry_val_size_{0};
    off_t current_file_size_{0};
    off_t allocated_size_{0};

    std::map<uint64_t, off_t> index_to_offset_;
    mutable std::mutex mtx_;
};