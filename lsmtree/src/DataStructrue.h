#pragma once
#include <vector>
#include <string>
#include <sstream>

//  单个 SSTable 文件的元数据物理身份证
struct FileMetaData {
    int level;          // 所处层级 (0-6)
    int file_number;    // 文件编号 (如 12 代表 data_L*_12.sst)
    size_t file_size;   // 文件物理字节大小
    std::string smallest_key;   // 文件包含的最小物理 Key
    std::string largest_key;    // 文件包含的最大物理 Key
};

//  专门为删除操作定义的坐标结构，语义清晰，防止混淆
struct DeletedFile {
    int level;
    int file_number;
};

//  LSM-Tree 版本变更记录载体（元数据微版本增量变更）
class VersionEdit {
public:
    void AddFile(int level, int file_number, size_t file_size, const std::string& smallest_key, const std::string& largest_key) {
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

    static std::string ToHex(const std::string& s) {
        if (s.empty()) return "-";
        static const char hex_chars[] = "0123456789ABCDEF";
        std::string res;
        res.reserve(s.size() * 2);
        for (unsigned char c : s) {
            res.push_back(hex_chars[c >> 4]);
            res.push_back(hex_chars[c & 0x0F]);
        }
        return res;
    }

    static std::string FromHex(const std::string& hex) {
        if (hex == "-" || hex.empty()) return "";
        std::string res;
        res.reserve(hex.size() / 2);
        for (size_t i = 0; i + 1 < hex.size(); i += 2) {
            auto hex_val = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                return 0;
            };
            res.push_back(static_cast<char>((hex_val(hex[i]) << 4) | hex_val(hex[i + 1])));
        }
        return res;
    }

    //  将版本变更增量序列化为明文，准备原子写入 MANIFEST 日志中
    // 格式定义："+ 层级 文件号 物理大小 最小键 最大键\n" 或 "- 层级 文件号\n"
    std::string EncodeToText() const {
        std::ostringstream oss;
        for (const auto& f : new_files_) {
            oss << "+ " << f.level << " " << f.file_number << " " 
                << f.file_size << " " << ToHex(f.smallest_key) << " " << ToHex(f.largest_key) << "\n";
        }
        for (const auto& f : deleted_files_) {
            oss << "- " << f.level << " " << f.file_number << "\n";
        }
        return oss.str();
    }

    std::vector<FileMetaData> new_files_;
    std::vector<DeletedFile> deleted_files_;
};