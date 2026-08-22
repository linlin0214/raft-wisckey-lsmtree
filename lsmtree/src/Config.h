#pragma once
#include <string>
#include <filesystem>

namespace config {
    inline const std::string TOMBSTONE = "__TOMBSTONE__";
    
    // 核心工业配置，全编译期常量
    constexpr size_t TARGET_FILE_SIZE = 16 << 20;       // 16MB (SST 大小阈值)
    constexpr size_t MAX_VLOG_SEGMENT_SIZE = 512 << 20;  // 512MB (vLog 单文件上限)
    
    constexpr int K_NUM_LEVELS = 7;                     // LSM-Tree 磁盘总层级（0 到 6）
    constexpr size_t K_MEMTABLE_THRESHOLD = 2 * 1024 * 1024; // 2MB 触发落盘的内存水位线
    constexpr size_t K_L0_COMPACT_TRIGGER = 4;          // L0 层合并触发文件数
}

struct Options {
    std::string db_dir = "./data";                   
    bool create_if_missing = true;                   
    size_t memtable_size = 12;                       
    size_t sst_file_size = config::TARGET_FILE_SIZE; 
    size_t vlog_segment_size = config::MAX_VLOG_SEGMENT_SIZE; 
    bool disable_wal = false; //支持分布式下彻底关闭单机 LSM-Tree 的 WAL
};

// 物理路径生成器：使用 std::filesystem 完全规避跨平台斜杠问题
class PathHelper {
public:
    static std::string GetSSTPath(const std::string& db_dir, int level, int file_id) {
        std::filesystem::path base(db_dir);
        std::string filename = "data_L" + std::to_string(level) + "_" + std::to_string(file_id) + ".sst";
        return (base / filename).lexically_normal().string();
    }

    static std::string GetWALPath(const std::string& db_dir) {
        std::filesystem::path base(db_dir);
        return (base / "production.wal").lexically_normal().string();
    }

    static std::string GetVLogPath(const std::string& db_dir) {
        std::filesystem::path base(db_dir);
        return (base / "vlog_storage").lexically_normal().string();
    }
};