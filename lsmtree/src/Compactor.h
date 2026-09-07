#pragma once
#include <vector>
#include <string>
#include <queue>
#include <atomic>
#include <memory>
#include <functional> 
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>
#include "SSTableReader.h"
#include "SSTableBuilder.h"
#include "Config.h"
#include "Slice.h"

struct MergeNode {
    std::string key;
    int idx; 

    bool operator>(const MergeNode& other) const {
        if (key != other.key) return Slice(key) > Slice(other.key);
        return idx > other.idx; 
    }
};

class Compactor {
public:
    std::vector<std::string> DoMajorCompaction(const std::string& base_dir, 
                                               const std::vector<std::shared_ptr<SSTableReader>>& inputs, 
                                               int target_level, 
                                               std::atomic<int>& file_id, 
                                               bool bottom) {
        std::vector<std::string> new_files;
        if (inputs.empty()) return new_files;

        std::priority_queue<MergeNode, std::vector<MergeNode>, std::greater<MergeNode>> pq;
        std::vector<std::unique_ptr<SSTableIterator>> iters;

        size_t total_estimated_keys = 0;
        for (size_t i = 0; i < inputs.size(); ++i) {
            if (inputs[i]) {
                total_estimated_keys += inputs[i]->GetBlockCount();
                auto it = std::make_unique<SSTableIterator>(inputs[i].get());
                if (it->Valid()) {
                    pq.push({it->Key().ToString(), static_cast<int>(i)}); 
                }
                iters.push_back(std::move(it));
            }
        }

        std::unique_ptr<SSTableBuilder> builder;
        std::string last_key; 
        bool has_last_key = false;

        const size_t size_threshold = (target_level <= 1) ? (16 << 20) : (64 << 20);
        size_t inputs_denom = std::max<size_t>(1, inputs.size() / 2);
        const size_t estimated_bloom_capacity = std::max<size_t>(20000, total_estimated_keys / inputs_denom);

        auto finalize_builder = [&](std::unique_ptr<SSTableBuilder>& b) {
            if (b) {
                b->Finish();
                b.reset(); 
            }
        };

        while (!pq.empty()) {
            auto cur = pq.top();
            pq.pop();

            auto& it = iters[cur.idx];

            if (has_last_key && Slice(cur.key) == Slice(last_key)) {
                // 重复 key 跳过
            } else {
                last_key = cur.key;
                has_last_key = true;

                std::string value = it->Value();

                if (!(bottom && value == config::TOMBSTONE)) {
                    if (!builder || builder->CurrentSize() > size_threshold) {
                        finalize_builder(builder);
                        
                        std::string pure_filename = "data_L" + std::to_string(target_level) + "_" + 
                                                   std::to_string(file_id.fetch_add(1)) + ".sst";
                        
                        std::string current_outname = base_dir + pure_filename;
                        
                        builder = std::make_unique<SSTableBuilder>();
                        builder->Start(current_outname, static_cast<int>(estimated_bloom_capacity)); 
                        
                        new_files.push_back(current_outname);
                    }
                    
                    builder->Add(Slice(cur.key), std::move(value)); 
                }
            }

            it->Next();
            if (it->Valid()) {
                pq.push({it->Key().ToString(), cur.idx}); 
            }
        }

        finalize_builder(builder);

        iters.clear();
        for (const auto& input : inputs) {
            if (input) {
                int fd = ::open(input->GetFilename().c_str(), O_RDONLY);
                if (fd >= 0) {
                    ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
                    ::close(fd);
                }
            }
        }

        for (const auto& fname : new_files) {
            int fd = ::open(fname.c_str(), O_RDONLY);
            if (fd >= 0) {
                ::fdatasync(fd);
                ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
                ::close(fd);
            }
        }

        return new_files; 
    }
};