#pragma once
#include <vector>
#include <string>
#include <sstream>

//  单个 SSTable 文件的元数据物理身份证
struct FileMetaData {
    int level;          // 所处层级 (0-6)
    int file_number;    // 文件编号 (如 12 代表 data_L*_12.sst)
    size_t file_size;   // 文件物理字节大小
    int smallest_key;   // 文件包含的最小物理 Key
    int largest_key;    // 文件包含的最大物理 Key
};

//  专门为删除操作定义的坐标结构，语义清晰，防止混淆
struct DeletedFile {
    int level;
    int file_number;
};

//  LSM-Tree 版本变更记录载体（元数据微版本增量变更）
class VersionEdit {
public:
    void AddFile(int level, int file_number, size_t file_size, int smallest_key, int largest_key) {
        new_files_.push_back({level, file_number, file_size, smallest_key, largest_key});
    }

    void DeleteFile(int level, int file_number) {
        deleted_files_.push_back({level, file_number});
    }

    //  清空当前增量变更，防范在循环复用同一个对象时发生脏状态污染
    void Clear() {
        new_files_.clear();
        deleted_files_.clear();
    }

    //  将版本变更增量序列化为明文，准备原子写入 MANIFEST 日志中
    // 格式定义："+ 层级 文件号 物理大小 最小键 最大键\n" 或 "- 层级 文件号\n"
    std::string EncodeToText() const {
        std::ostringstream oss;
        
        for (const auto& f : new_files_) {
            oss << "+ " << f.level << " " << f.file_number << " " 
                << f.file_size << " " << f.smallest_key << " " << f.largest_key << "\n";
        }
        
        for (const auto& f : deleted_files_) {
            oss << "- " << f.level << " " << f.file_number << "\n";
        }
        
        return oss.str();
    }

    std::vector<FileMetaData> new_files_;
    std::vector<DeletedFile> deleted_files_;
};