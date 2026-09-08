#pragma once
#include "Arena.h"
#include "VLogStruct.h"
#include "skiplist.h"
#include "WalManager.h"
#include "SSTableBuilder.h"
#include "SSTableReader.h"
#include "Compactor.h"
#include "Config.h"
#include "ValueLog.h"
#include "DataStructrue.h"
#include "ManifestManager.h"
#include "LRUCache.h"
#include "ThreadWrite.h"
#include "ThreadPool.h"
#include "Iterator.h"
#include "Slice.h"
#include "RateLimiter.h"
#include <cstddef>
#include <memory>
#include <vector>
#include <string>
#include <filesystem>
#include <algorithm>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cmath>
#include <deque>
#include <climits>
#include <fcntl.h>
#include <unistd.h>
#include <random>

static constexpr size_t max_batch_size = 64;
namespace fs = std::filesystem;

struct ImmContext {
    std::unique_ptr<Arena> arena;
    std::unique_ptr<skiplist> mem;

    ImmContext(int max_level = 12) {
        arena = std::make_unique<Arena>();
        mem = std::make_unique<skiplist>(max_level, arena.get());
    }
};

class DB {
public:
    explicit DB(const std::string& db_dir, const Options& options = Options()) 
        : base_dir_([&]() {
              std::string dir = db_dir;
              if (!dir.empty() && dir.back() != '/' && dir.back() != '\\') {
                  dir += "/";
              }
              return dir;
          }()), 
          options_(options),
          arena_(),                     
          memtable_(12, &arena_),
          wal_(base_dir_ + "production.wal", false, options_.disable_wal), 
          vlog_(base_dir_ + "vlog_storage"), 
          manifest_manager_(base_dir_), 
          high_pri_pool(1), // 负责 Flush
          low_pri_pool(2),  // 专职负责 Compaction 和 GC
          shutting_down_(false),
          is_compacting_(false),
          is_gcing_(false),
          file_id_(0),
          // 仅引入令牌桶限速器平抑磁盘写入带宽，不额外新建线程
          gc_rate_limiter_(std::make_unique<TokenBucketRateLimiter>(options_.vlog_gc_rate_limit_bytes)) {
        
        std::filesystem::create_directories(base_dir_);
        levels_.resize(config::K_NUM_LEVELS); 
        manifest_manager_.Recover(levels_);

        int max_id = 0;
        for (const auto& level : levels_) {
            for (const auto& r : level) {
                max_id = std::max(max_id, ExtractId(r->GetFilename()));
            }
        }
        file_id_.store(max_id + 1); 

        if (!options_.disable_wal) {
            wal_.recovery(memtable_); 
        }
    }

    ~DB() {
        shutting_down_.store(true);
        {
            std::lock_guard<std::mutex> lock(cv_mutex_);
            put_cv_.notify_all();
        }
        // high_pri_pool 与 low_pri_pool 会在自身析构函数中自动唤醒并安全 join 全部 Worker
    }

    void Put(const Slice& key, const std::string& value) {
        ThreadWrite::Writer w(key, &value);
        thread_write_.JoinBatchGroup(&w);
        if (w.state.load(std::memory_order_acquire) == ThreadWrite::STATE_FOLLOWER) {
            thread_write_.AwaitState(&w, ThreadWrite::STATE_COMPLETE);
            return;
        }

        std::vector<ThreadWrite::Writer*> batch_group;
        thread_write_.EnterAsBatchGroupLeader(batch_group);

        std::vector<std::string> encode_ptr;
        encode_ptr.reserve(batch_group.size());
        {  
            std::unique_lock<std::mutex> lock(vlog_mutex_);
            for (auto writer : batch_group) {
                VLogPointer vlog_ptr = vlog_.Append(Slice(writer->key), *(writer->value));
                encode_ptr.push_back(vlog_ptr.Encode());
            }
        }

        {
            std::unique_lock<std::mutex> lock(rw_mutex_);
            ThreadWrite::WriteBatch batch;
            batch.entries.reserve(batch_group.size());

            for (size_t i = 0; i < batch_group.size(); ++i) {
                const std::string& k = batch_group[i]->key;
                const std::string& ptr = encode_ptr[i];
                batch.entries.push_back({k, ptr});
                memtable_.insert(Slice(k), ptr, memtable_.RandomLevel());  
            }

            if (!options_.disable_wal) {
                wal_.LogBatch(batch);
            }

            MaybeSwapMemtable(lock);
        }
        thread_write_.ExitAsBatchGroupLeader(batch_group);
    }

    void Delete(const Slice& key) {
        Put(key, config::TOMBSTONE); 
    }

    std::string Get(const Slice& key) {
        std::unique_lock<std::mutex> lock(rw_mutex_);
        
        std::string internal_val = InternalGetPtrNoLock(key);
        if (internal_val.empty() || internal_val == config::TOMBSTONE) return "NOT_FOUND";

        VLogPointer ptr = VLogPointer::Decode(internal_val);
        if (ptr.size == 0) return "NOT_FOUND"; 

        std::lock_guard<std::mutex> vlog_lock(vlog_mutex_);
        return vlog_.Read(ptr); 
    }

    static void DestroyDB(const std::string& wal_path) {
        std::error_code ec; 
        if (std::filesystem::exists(wal_path)) {
            std::filesystem::remove(wal_path, ec);
        }
        std::string vlog_dir = wal_path + "_vlog";
        if (std::filesystem::exists(vlog_dir)) {
            std::filesystem::remove_all(vlog_dir, ec);
        }
        std::filesystem::path p(wal_path);
        std::string target_dir = p.has_parent_path() ? p.parent_path().string() : ".";
        if (std::filesystem::exists(target_dir)) {
            for (const auto& entry : std::filesystem::directory_iterator(target_dir)) {
                if (entry.path().extension() == ".sst") {
                    std::filesystem::remove(entry.path(), ec);
                }
            }
        }
    }

    std::unique_ptr<MergingIterator> NewMergingIterator() {
        std::vector<std::shared_ptr<ImmContext>> imm_snapshot;
        std::vector<std::vector<std::shared_ptr<SSTableReader>>> levels_snapshot;

        {
            std::unique_lock<std::mutex> lock(rw_mutex_);

            if (!memtable_.empty()) {
                auto imm_ctx = std::make_shared<ImmContext>(12);
                arena_.Swap(*(imm_ctx->arena));
                imm_ctx->mem->StealFrom(memtable_);
                memtable_.Clear();
                imm_queue_.push_back(imm_ctx);
                imm_queue_size_.fetch_add(1);
            }

            imm_snapshot.assign(imm_queue_.begin(), imm_queue_.end());
            levels_snapshot = levels_;
        }

        std::vector<std::unique_ptr<StorageIterator>> iters;
        
        for (auto it = imm_snapshot.rbegin(); it != imm_snapshot.rend(); ++it) {
            iters.push_back(std::make_unique<MemTableIteratorAdaptor>((*it)->mem->Begin(), *it));
        }

        if (!levels_snapshot.empty() && !levels_snapshot[0].empty()) {
            auto l0_files = levels_snapshot[0];
            std::sort(l0_files.begin(), l0_files.end(), [this](const auto& a, const auto& b) {
                return ExtractId(a->GetFilename()) > ExtractId(b->GetFilename());
            });
            for (const auto& reader : l0_files) {
                iters.push_back(std::make_unique<SSTableIteratorAdaptor>(reader));
            }
        }

        for (size_t l = 1; l < levels_snapshot.size(); ++l) {
            for (const auto& reader : levels_snapshot[l]) {
                iters.push_back(std::make_unique<SSTableIteratorAdaptor>(reader));
            }
        }

        return std::make_unique<MergingIterator>(std::move(iters));
    }

    std::string ReadVLog(const VLogPointer& ptr) {
        return vlog_.Read(ptr);
    }

    void PutBatch(const std::vector<std::pair<std::string, std::string>>& kvs) {
        if (kvs.empty()) return;

        std::vector<std::pair<std::string, std::string>> ptr_kvs;
        ptr_kvs.reserve(kvs.size());

        {
            std::lock_guard<std::mutex> vlog_lock(vlog_mutex_);
            for (const auto& [key, value] : kvs) {
                if (value == config::TOMBSTONE) {
                    ptr_kvs.push_back({key, config::TOMBSTONE});
                } else {
                    VLogPointer ptr = vlog_.Append(Slice(key), value);
                    ptr_kvs.push_back({key, ptr.Encode()});
                }
            }
        }

        {
            std::unique_lock<std::mutex> lsm_lock(rw_mutex_);
            for (const auto& [key, encoded_ptr] : ptr_kvs) {
                memtable_.insert(Slice(key), encoded_ptr, memtable_.RandomLevel());
            }
            MaybeSwapMemtable(lsm_lock);
        }
    }

    void Sync() {
        std::unique_lock<std::mutex> lock(rw_mutex_);
        if (!options_.disable_wal) {
            wal_.Sync();
        }
        vlog_.Sync();
    }

    // 允许外部主动向 low_pri_pool 线程池投递一次 GC 检查
    void MaybeTriggerGCAsync() {
        if (shutting_down_.load(std::memory_order_relaxed)) return;
        low_pri_pool.Enqueue([this]() {
            MaybeTriggerGC();
        });
    }

private:
    std::string InternalGetPtrNoLock(const Slice& key) {
        auto node = memtable_.search(key); 
        if (node) return node->GetValue();
        
        for (auto it = imm_queue_.rbegin(); it != imm_queue_.rend(); ++it) {
            auto imm_node = (*it)->mem->search(key);
            if (imm_node) return imm_node->GetValue();
        }
        
        for (int i = (int)levels_[0].size() - 1; i >= 0; --i) {
            if (!levels_[0][i]->MightContain(key)) continue;
            std::string v = levels_[0][i]->Search(key, &lru_cache_); 
            if (!v.empty()) return v;
        }
        
        for (int l = 1; l < config::K_NUM_LEVELS; ++l) {
            auto it = std::lower_bound(levels_[l].begin(), levels_[l].end(), key,
                [](const std::shared_ptr<SSTableReader>& r, const Slice& k) { 
                    return Slice(r->GetMaxKey()) < k; 
                });
            
            if (it != levels_[l].end() && key >= Slice((*it)->GetMinKey())) {
                if (!(*it)->MightContain(key)) continue;
                std::string v = (*it)->Search(key, &lru_cache_);
                if (!v.empty()) return v;
            }
        }
        return "";
    }

    void MaybeTriggerCompaction() {
        if (shutting_down_.load()) return;
        if (is_compacting_.exchange(true)) return;
        
        struct compactionguard {
            std::atomic<bool>& flag;
            ~compactionguard() { flag.store(false); }
        } guard{is_compacting_};

        bool need_compact = false;
        {
            std::unique_lock<std::mutex> lock_(rw_mutex_);
            if (levels_[0].size() >= 4) {
                need_compact = true;
            }
        }
        if (need_compact) {
            MajorCompaction();
        }
    }

    void MajorCompaction() {
        bool has_work = true;
        while (has_work && !shutting_down_.load()) {
            has_work = false; 
            for (int i = 0; i < 6; ++i) {
                std::vector<std::shared_ptr<SSTableReader>> level_snapshot;
                {
                    std::unique_lock<std::mutex> lock(rw_mutex_);
                    level_snapshot = levels_[i];
                }

                size_t current_level_bytes = 0;
                for (const auto& r : level_snapshot) {
                    std::error_code ec;
                    current_level_bytes += std::filesystem::file_size(r->GetFilename(), ec);
                }

                bool trigger_compaction = false;
                if (i == 0) {
                    if (level_snapshot.size() >= 4) trigger_compaction = true;
                } else {
                    size_t threshold_bytes = 10 * 1024 * 1024 * static_cast<size_t>(std::pow(10, i - 1));
                    if (current_level_bytes >= threshold_bytes) trigger_compaction = true;
                }

                if (!trigger_compaction) continue; 

                has_work = true; 
                std::vector<std::shared_ptr<SSTableReader>> inputs_curr;
                std::vector<std::shared_ptr<SSTableReader>> inputs_next;
                std::string min_k, max_k;
                bool first_k = true;

                {
                    std::unique_lock<std::mutex> lock(rw_mutex_);
                    if (i == 0) {
                        if (levels_[i].size() < 4) continue;
                        inputs_curr = levels_[i]; 
                    } else {
                        size_t num_to_merge = std::max<size_t>(2, levels_[i].size() / 2);
                        for (size_t n = 0; n < num_to_merge && n < levels_[i].size(); ++n) {
                            inputs_curr.push_back(levels_[i][n]);  
                        }
                    }
                    
                    for (const auto& r : inputs_curr) {
                        if (first_k) {
                            min_k = r->GetMinKey();
                            max_k = r->GetMaxKey();
                            first_k = false;
                        } else {
                            if (Slice(r->GetMinKey()) < Slice(min_k)) min_k = r->GetMinKey();
                            if (Slice(r->GetMaxKey()) > Slice(max_k)) max_k = r->GetMaxKey();
                        }
                    }
                    
                    for (const auto& r : levels_[i + 1]) {
                        if (!(Slice(r->GetMaxKey()) < Slice(min_k) || Slice(r->GetMinKey()) > Slice(max_k))) {
                            inputs_next.push_back(r);
                        }
                    }
                } 

                std::vector<std::shared_ptr<SSTableReader>> all_inputs;
                if (i == 0) {
                    std::vector<std::shared_ptr<SSTableReader>> rev_curr = inputs_curr;
                    std::reverse(rev_curr.begin(), rev_curr.end());
                    all_inputs = rev_curr;
                } else {
                    all_inputs = inputs_curr;
                }
                all_inputs.insert(all_inputs.end(), inputs_next.begin(), inputs_next.end());

                std::vector<std::string> new_files;
                try {
                    Compactor c;
                    new_files = c.DoMajorCompaction(base_dir_, all_inputs, i + 1, file_id_, (i + 1 == 6));
                } catch (const std::exception& e) {
                    std::cerr << "[Compaction Fatal] 合并过程抛出异常: " << e.what() << std::endl;
                    continue; 
                }

                if (new_files.empty() && !all_inputs.empty()) {
                    continue;
                }

                VersionEdit edit;
                for (const auto& r : inputs_curr) edit.DeleteFile(i, ExtractId(r->GetFilename()));
                for (const auto& r : inputs_next) edit.DeleteFile(i + 1, ExtractId(r->GetFilename()));

                std::vector<std::shared_ptr<SSTableReader>> new_readers;
                for (const auto& name : new_files) {
                    auto reader = std::make_shared<SSTableReader>(name);
                    new_readers.push_back(reader);
                    std::error_code ec;
                    size_t fsize = std::filesystem::file_size(name, ec); 
                    edit.AddFile(i + 1, ExtractId(name), fsize, reader->GetMinKey(), reader->GetMaxKey()); 
                }

                manifest_manager_.LogAndApply(edit);

                {
                    std::unique_lock<std::mutex> lock(rw_mutex_);
                    auto& lv_curr = levels_[i];
                    lv_curr.erase(std::remove_if(lv_curr.begin(), lv_curr.end(),
                        [&](const std::shared_ptr<SSTableReader>& r) {
                            return std::find(inputs_curr.begin(), inputs_curr.end(), r) != inputs_curr.end();
                        }), lv_curr.end());

                    auto& lv_next = levels_[i + 1];
                    lv_next.erase(std::remove_if(lv_next.begin(), lv_next.end(),
                        [&](const std::shared_ptr<SSTableReader>& r) {
                            return std::find(inputs_next.begin(), inputs_next.end(), r) != inputs_next.end();
                        }), lv_next.end());

                    for (const auto& reader : new_readers) {
                        lv_next.push_back(reader);
                    }

                    std::sort(lv_next.begin(), lv_next.end(), 
                        [](const std::shared_ptr<SSTableReader>& a, const std::shared_ptr<SSTableReader>& b) { 
                            return Slice(a->GetMinKey()) < Slice(b->GetMinKey()); 
                        });
                }

                for (const auto& r : all_inputs) {
                    std::error_code ec;
                    fs::remove(r->GetFilename(), ec);
                }
            }
        }
    }

    void MaybeTriggerGC() {
        if (shutting_down_.load()) return;
        if (is_gcing_.exchange(true)) return;
        
        struct gcguard {
            std::atomic<bool>& flag;
            ~gcguard() { flag.store(false); }
        } guard{is_gcing_};
        
        GCWork();
    }

    // 铁律 3：纯被动触发 + 严格垃圾率门禁，杜绝 0 垃圾强行兜底
    void GCWork() {
        struct SampleItem {
            std::string key;
            VLogPointer pos;
        };
        struct GCEntry {
            std::string key;
            std::string value;
            VLogPointer current_pos;
        };

        if (shutting_down_.load()) return;

        std::vector<uint32_t> inactive_vlogs;
        std::string vlog_dir = base_dir_ + "vlog_storage";
        if (!std::filesystem::exists(vlog_dir)) return;

        uint32_t active_fid = vlog_.GetCurrentFileId();
        for (const auto& entry : std::filesystem::directory_iterator(vlog_dir)) {
            if (entry.path().extension() == ".log") {
                uint32_t fid = ExtractId(entry.path().filename().string());
                if (fid > 0 && fid != active_fid) {
                    inactive_vlogs.push_back(fid);
                }
            }
        }

        // 积压段数少于 4 个时直接跳过，绝不打扰前台
        if (inactive_vlogs.size() < 4) return;

        std::sort(inactive_vlogs.begin(), inactive_vlogs.end());

        double dynamic_threshold = 0.50; 
        if (inactive_vlogs.size() > 50) {
            dynamic_threshold = 0.30; 
        }

        uint32_t best_victim_id = 0;
        double max_garbage_ratio = -1.0;

        std::vector<uint32_t> sample_pool = inactive_vlogs;
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(sample_pool.begin(), sample_pool.end(), g);
            
        int sample_count = std::min<int>(5, static_cast<int>(sample_pool.size()));
            
        for (int i = 0; i < sample_count; ++i) {
            uint32_t fid = sample_pool[i];
            std::string path = vlog_.GetPath(fid);
            int fd = ::open(path.c_str(), O_RDONLY);
            if (fd < 0) continue;

            std::vector<SampleItem> samples;
            samples.reserve(101);

            uint64_t current_offset = 0;
            uint16_t key_len;
            uint32_t val_size;

            while (samples.size() < 100 && ::pread(fd, &key_len, sizeof(key_len), current_offset) == sizeof(key_len)) {
                if (shutting_down_.load()) break;
                if (::pread(fd, &val_size, sizeof(val_size), current_offset + sizeof(key_len)) != sizeof(val_size)) break;
                
                std::string k_str(key_len, '\0');
                if (key_len > 0 && ::pread(fd, &k_str[0], key_len, current_offset + 6) != static_cast<ssize_t>(key_len)) break;

                uint64_t val_offset = current_offset + 6 + key_len;
                uint32_t bytes_size = 6 + key_len + val_size;
                VLogPointer currentpos = {fid, val_offset, val_size};
                samples.push_back({std::move(k_str), currentpos});
                current_offset += bytes_size;
            }
            ::close(fd);

            if (samples.empty()) continue;

            int stale_entries = 0;
            {
                std::unique_lock<std::mutex> sample_lock(rw_mutex_);
                for (auto& sample : samples) {
                    bool is_stale = true;
                    std::string index_ptr_str_ = InternalGetPtrNoLock(Slice(sample.key));
                    if (!index_ptr_str_.empty() && index_ptr_str_ != config::TOMBSTONE) {
                        VLogPointer index_ptr = VLogPointer::Decode(index_ptr_str_);
                        if (index_ptr.file_id == sample.pos.file_id && index_ptr.offset == sample.pos.offset) {
                            is_stale = false;
                        }
                    }
                    if (is_stale) stale_entries++;
                }
            }
            double ratio = static_cast<double>(stale_entries) / samples.size();
            if (ratio > max_garbage_ratio) {
                max_garbage_ratio = ratio;
                best_victim_id = fid;
            }
        }

        // 严格硬拦截：垃圾率不足阈值或低于 20% 时直接返回，彻底杜绝 0 垃圾无意义重写
        if (max_garbage_ratio < dynamic_threshold || best_victim_id == 0 || max_garbage_ratio <= 0.20) {
            return; 
        }

        std::cout << "\n[GC]  锁定高垃圾率目标: vlog_" << best_victim_id << ".log | 预计垃圾率: " 
                  << (max_garbage_ratio * 100) << "%" << std::endl;

        uint32_t victim_id = best_victim_id;
        std::string path = vlog_.GetPath(victim_id);
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return;

        uint64_t current_offset = 0;
        uint64_t chunk_bytes = 0;
        uint64_t chunk_start_offset = 0;

        uint16_t key_len;
        uint32_t val_size;
        int valid_count = 0, stale_count = 0;

        std::vector<GCEntry> gc_batch;
        gc_batch.reserve(256);

        auto FlushGCBatch = [&](std::vector<GCEntry>& batch) {
            if (batch.empty()) return;

            // 核心接入：使用令牌桶限速器按字节控速，压平磁盘 I/O 尖刺
            size_t batch_bytes = 0;
            for (const auto& item : batch) {
                batch_bytes += sizeof(uint16_t) + sizeof(uint32_t) + item.key.size() + item.value.size();
            }
            if (gc_rate_limiter_) {
                gc_rate_limiter_->Request(batch_bytes);
            }

            std::vector<VLogPointer> new_ptrs;
            new_ptrs.reserve(batch.size());
            {
                std::lock_guard<std::mutex> vlog_lock(vlog_mutex_);
                for (const auto& item : batch) {
                    new_ptrs.push_back(vlog_.Append(Slice(item.key), item.value));
                }
            }

            {
                std::unique_lock<std::mutex> write_lock(rw_mutex_);
                ThreadWrite::WriteBatch wal_batch;

                for (size_t i = 0; i < batch.size(); ++i) {
                    std::string double_check_str = InternalGetPtrNoLock(Slice(batch[i].key));
                    if (!double_check_str.empty() && double_check_str != config::TOMBSTONE) {
                        VLogPointer check_ptr = VLogPointer::Decode(double_check_str);
                        
                        if (check_ptr.file_id == batch[i].current_pos.file_id && check_ptr.offset == batch[i].current_pos.offset) {
                            std::string new_ptr_encoded = new_ptrs[i].Encode();
                            wal_batch.entries.push_back({batch[i].key, new_ptr_encoded});
                            memtable_.insert(Slice(batch[i].key), new_ptr_encoded, memtable_.RandomLevel());
                            valid_count++;
                            continue;
                        }
                    }
                    stale_count++;
                }

                if (!wal_batch.entries.empty() && !options_.disable_wal) {
                    wal_.LogBatch(wal_batch);
                }
                MaybeSwapMemtable(write_lock);
            }
            batch.clear();
            std::this_thread::sleep_for(std::chrono::microseconds(200)); // 让路前台写入
        };

        while (::pread(fd, &key_len, sizeof(key_len), current_offset) == sizeof(key_len)) {
            if (shutting_down_.load()) break;

            if (::pread(fd, &val_size, sizeof(val_size), current_offset + sizeof(key_len)) != sizeof(val_size)) break;

            std::string key_str(key_len, '\0');
            if (key_len > 0) {
                if (::pread(fd, &key_str[0], key_len, current_offset + 6) != static_cast<ssize_t>(key_len)) break;
            }

            std::string value(val_size, '\0');
            uint64_t val_offset = current_offset + 6 + key_len;
            if (val_size > 0) {
                if (::pread(fd, &value[0], val_size, val_offset) != static_cast<ssize_t>(val_size)) break;
            }

            VLogPointer current_pos = {victim_id, val_offset, val_size};
            uint32_t entry_bytes = 6 + key_len + val_size;
            current_offset += entry_bytes;
            chunk_bytes += entry_bytes;

            if (chunk_bytes >= 16 * 1024 * 1024) {
                ::posix_fadvise(fd, chunk_start_offset, chunk_bytes, POSIX_FADV_DONTNEED);
                chunk_start_offset = current_offset;
                chunk_bytes = 0;
            }

            std::string indexed_ptr_str;
            {
                std::unique_lock<std::mutex> lock(rw_mutex_);
                indexed_ptr_str = InternalGetPtrNoLock(Slice(key_str));
            }

            if (!indexed_ptr_str.empty() && indexed_ptr_str != config::TOMBSTONE) {
                VLogPointer indexed_ptr = VLogPointer::Decode(indexed_ptr_str);
                if (indexed_ptr.file_id == current_pos.file_id && indexed_ptr.offset == current_pos.offset) {
                    gc_batch.push_back({std::move(key_str), std::move(value), current_pos});
                    if (gc_batch.size() >= 256) {
                        FlushGCBatch(gc_batch);
                    }
                    continue;
                }
            }
            stale_count++;
        }

        FlushGCBatch(gc_batch);

        if (chunk_bytes > 0) {
            ::posix_fadvise(fd, chunk_start_offset, chunk_bytes, POSIX_FADV_DONTNEED);
        }
        ::close(fd);

        // 仅在 vlog_mutex_ 保护下注销并删除文件，绝不持有 rw_mutex_ 阻塞前台读写
        {
            std::lock_guard<std::mutex> vlog_lock(vlog_mutex_);
            vlog_.RemoveSegment(victim_id);
        }

        std::cout << "[GC] 清洗完成！搬运有效数据: " << valid_count 
                  << " 条 | 丢弃垃圾碎片: " << stale_count << " 条。" << std::endl;
    }

    int ExtractId(const std::string& filename) {
        size_t last_underscore = filename.find_last_of('_');
        size_t last_dot = filename.find_last_of('.');
        if (last_underscore != std::string::npos && last_dot != std::string::npos) {
            std::string id_str = filename.substr(last_underscore + 1, last_dot - last_underscore - 1);
            try {
                return std::stoi(id_str);
            } catch (...) { return 0; }
        }
        return 0;
    }

    // Immutable 队列门禁放宽至 8，提供充足平滑写缓冲
    void MaybeSwapMemtable(std::unique_lock<std::mutex>& lsm_lock) {
        if (arena_.memory_usage() > config::K_MEMTABLE_THRESHOLD) {
            
            while (imm_queue_size_.load() >= 8 && !shutting_down_.load()) {
                lsm_lock.unlock(); 
                {
                    std::unique_lock<std::mutex> cv_lock(cv_mutex_);
                    put_cv_.wait(cv_lock, [this] { return imm_queue_size_.load() < 8 || shutting_down_.load(); });
                }
                lsm_lock.lock(); 
            }
            
            if (shutting_down_.load()) return;

            auto imm_ctx = std::make_shared<ImmContext>(12);
            arena_.Swap(*(imm_ctx->arena));
            imm_ctx->mem->StealFrom(memtable_);
            memtable_.Clear();
            
            imm_queue_.push_back(imm_ctx);
            imm_queue_size_.fetch_add(1);
            
            lsm_lock.unlock(); 
            
            high_pri_pool.Enqueue([this]() {
                SingleFlushTask();
            });
        }
    }

    // 先挂载新 SSTable，再弹出 Immutable 队列，0 读黑洞
    void SingleFlushTask() {
        std::shared_ptr<ImmContext> imm_to_flush;
        {
            std::unique_lock<std::mutex> lock_(rw_mutex_);
            if (imm_queue_.empty()) return;
            imm_to_flush = imm_queue_.front();
        }

        int new_file_id = file_id_.fetch_add(1);
        std::string name = base_dir_ + "data_L0_" + std::to_string(new_file_id) + ".sst";

        SSTableBuilder local_builder;
        local_builder.Build(*(imm_to_flush->mem), name);

        {
            std::unique_lock<std::mutex> lock_(rw_mutex_);
            levels_[0].push_back(std::make_shared<SSTableReader>(name));

            VersionEdit vedt_;
            vedt_.AddFile(0, new_file_id, local_builder.GetFileSize(), local_builder.GetMinKey(), local_builder.GetMaxKey());
            manifest_manager_.LogAndApply(vedt_);

            imm_queue_.pop_front();
            imm_queue_size_.fetch_sub(1);
        }

        {
            std::unique_lock<std::mutex> lock_(cv_mutex_);
            put_cv_.notify_all();
        }

        // 核心优化：解耦投递至 low_pri_pool(2)，充分利用两个线程分别并发运行 Compaction 和 GC
        low_pri_pool.Enqueue([this]() {
            MaybeTriggerCompaction();
        });
        low_pri_pool.Enqueue([this]() {
            MaybeTriggerGC();
        });
    }

private:
    Options options_;
    std::string base_dir_; 

    Arena arena_;
    skiplist memtable_;
    std::deque<std::shared_ptr<ImmContext>> imm_queue_;
    std::atomic<int> imm_queue_size_{0};
    
    ShardedLRUCache lru_cache_{10000, 16};
    
    WalManager wal_;
    ValueLog vlog_;
    SSTableBuilder builder_;
    ManifestManager manifest_manager_;
    ThreadWrite thread_write_;

    std::vector<std::vector<std::shared_ptr<SSTableReader>>> levels_;
    
    std::mutex queue_mutex_;
    std::mutex rw_mutex_;   
    std::mutex vlog_mutex_;                
    std::mutex cv_mutex_;      

    std::condition_variable put_cv_;       
    
    ThreadPool high_pri_pool; // 专职负责 Flush (1 线程)
    ThreadPool low_pri_pool;  // 专职负责 Compaction 和 GC (2 线程)
    
    std::atomic<bool> shutting_down_;
    std::atomic<bool> is_compacting_;
    std::atomic<bool> is_gcing_;
    std::atomic<int> file_id_;

    std::unique_ptr<TokenBucketRateLimiter> gc_rate_limiter_;
};