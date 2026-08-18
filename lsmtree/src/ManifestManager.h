#pragma once
#include "DataStructrue.h"
#include "SSTableReader.h"
#include <fstream>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <mutex>

namespace fs = std::filesystem;

class ManifestManager {
public:
    ManifestManager(const std::string& db_dir) : db_dir_(db_dir) {
        if (!db_dir_.empty() && db_dir_.back() != '/' && db_dir_.back() != '\\') {
            db_dir_ += "/";
        }
    }

    //  原子性向物理日志账本追加一条微版本 Edit，通过 apply_mtx_ 强锁，防范 Flush 线程与 Compaction 并发交织写坏日志
    void LogAndApply(const VersionEdit& edit) {
        std::lock_guard<std::mutex> lock(apply_mtx_); 
        
        std::string manifest_path = db_dir_ + current_manifest_name_;
        std::ofstream out(manifest_path, std::ios::app | std::ios::binary);
        
        if (!out.is_open()) {
            std::cerr << "[Manifest] 错误：无法打开账本文件 " << current_manifest_name_ << " 进行原子追加" << std::endl;
            return;
        }

        out << edit.EncodeToText();
        out.flush(); 
    }

    //  数据库崩溃与正常冷启动元数据恢复主逻辑
    //  核心纠偏：形参完全对齐为 std::shared_ptr 的多层容器，彻底消灭 UAF 与内存泄露
    void Recover(std::vector<std::vector<std::shared_ptr<SSTableReader>>>& db_levels) {
        std::string current_file = db_dir_ + "CURRENT";
        
        // 1. 首次建库初始化
        if (!fs::exists(current_file)) {
            current_manifest_name_ = "MANIFEST-000001";
            std::ofstream out_manifest(db_dir_ + current_manifest_name_);
            std::ofstream out_current(current_file);
            out_current << current_manifest_name_ << "\n";
            std::cout << "[Manifest] 初始化：创建活跃账本 " << current_manifest_name_ << std::endl;
            return;
        }

        // 2. 读取 CURRENT 指针，定位当前的 Manifest 文件
        std::ifstream in_current(current_file);
        if (!(in_current >> current_manifest_name_)) {
            current_manifest_name_ = "MANIFEST-000001";
        }
        std::cout << "[Manifest] 正在从 " << current_manifest_name_ << " 重塑数据库内存视图..." << std::endl;

        // 阶段一：版本流投影推演 (Version Projection)
        std::unordered_set<std::string> live_files; 

        std::ifstream in_manifest(db_dir_ + current_manifest_name_, std::ios::binary);
        std::string line;
        while (std::getline(in_manifest, line)) {
            if (line.empty()) continue;
            //  完美剥离 Windows 回车符，提供最强跨平台解析兼容性
            if (!line.empty() && line.back() == '\r') line.pop_back();

            std::istringstream iss(line);
            char op;
            iss >> op;

            if (op == '+') {
                int level, file_num, smallest, largest;
                size_t size;
                if (iss >> level >> file_num >> size >> smallest >> largest) {
                    // 以 "层级_文件号" 作为存活映射键
                    live_files.insert(std::to_string(level) + "_" + std::to_string(file_num));
                }
            } else if (op == '-') {
                int level, file_num;
                if (iss >> level >> file_num) {
                    live_files.erase(std::to_string(level) + "_" + std::to_string(file_num));
                }
            }
        }

        // 阶段二：重构多层内存视图，使用 std::make_shared 代替裸指针堆开销，防止泄露
        for (const auto& key : live_files) {
            size_t underscore_pos = key.find('_');
            int level = std::stoi(key.substr(0, underscore_pos));
            int file_num = std::stoi(key.substr(underscore_pos + 1));

            std::string sst_name = db_dir_ + "data_L" + std::to_string(level) + "_" + std::to_string(file_num) + ".sst";
            
            if (fs::exists(sst_name)) {
                if (db_levels.size() <= static_cast<size_t>(level)) {
                    db_levels.resize(level + 1);
                }
                //  利用 make_shared 接管 SSTable 物理句柄所有权
                db_levels[level].push_back(std::make_shared<SSTableReader>(sst_name));
            } else {
                std::cerr << "[Manifest] 严重警告：物理文件 " << sst_name << " 丢失！系统视图暂作空处理" << std::endl;
            }
        }

        // 阶段三：层级排序（L0 层降序最新优先，L1-L6 层按 key range 升序）
        for (size_t i = 0; i < db_levels.size(); ++i) {
            if (i == 0) {
                std::sort(db_levels[i].begin(), db_levels[i].end(), [](const std::shared_ptr<SSTableReader>& a, const std::shared_ptr<SSTableReader>& b) {
                    return ExtractId(a->GetFilename()) > ExtractId(b->GetFilename());
                });
            } else {
                std::sort(db_levels[i].begin(), db_levels[i].end(), [](const std::shared_ptr<SSTableReader>& a, const std::shared_ptr<SSTableReader>& b) {
                    return a->GetMinKey() < b->GetMinKey();
                });
            }
        }

        // 阶段四：物理垃圾回收 (清理数据库重启时由于断电遗留在磁盘上的半吊子、未记录在案的 SST)
        int junk_count = 0;
        for (const auto& entry : fs::directory_iterator(db_dir_)) {
            if (entry.path().extension() == ".sst") {
                std::string filename = entry.path().filename().string();
                int level, file_num;
                if (sscanf(filename.c_str(), "data_L%d_%d.sst", &level, &file_num) == 2) {
                    std::string lookup_key = std::to_string(level) + "_" + std::to_string(file_num);
                    if (live_files.find(lookup_key) == live_files.end()) {
                        fs::remove(entry.path());
                        junk_count++;
                    }
                }
            }
        }
        std::cout << "[Manifest] 元数据装载完成。存活 SST 视图数: " << live_files.size() 
                  << " | 物理清理残缺垃圾文件: " << junk_count << std::endl;
    }

private:
    std::string db_dir_;
    std::string current_manifest_name_;
    std::mutex apply_mtx_; 

    static int ExtractId(const std::string& full_path) {
        std::string filename = fs::path(full_path).filename().string();
        size_t last_underscore = filename.find_last_of('_');
        size_t last_dot = filename.find_last_of('.');
        if (last_underscore != std::string::npos && last_dot != std::string::npos) {
            std::string id_str = filename.substr(last_underscore + 1, last_dot - last_underscore - 1);
            try { return std::stoi(id_str); } catch (...) { return 0; }
        }
        return 0;
    }
};