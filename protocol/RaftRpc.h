#pragma once

#include <cstdint>
#include <string>
#include <cstring>
#include <string_view>
#include <vector>

namespace raft_rpc {

// 0. 网络字节序大端编解码辅助工具函数 (Zero-Heap Overhead)
inline void EncodeUint64(char* dst, uint64_t val) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    ptr[0] = static_cast<uint8_t>((val >> 56) & 0xFF);
    ptr[1] = static_cast<uint8_t>((val >> 48) & 0xFF);
    ptr[2] = static_cast<uint8_t>((val >> 40) & 0xFF);
    ptr[3] = static_cast<uint8_t>((val >> 32) & 0xFF);
    ptr[4] = static_cast<uint8_t>((val >> 24) & 0xFF);
    ptr[5] = static_cast<uint8_t>((val >> 16) & 0xFF);
    ptr[6] = static_cast<uint8_t>((val >> 8)  & 0xFF);
    ptr[7] = static_cast<uint8_t>(val & 0xFF);
}

inline uint64_t DecodeUint64(const char* src) {
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(src);
    return (static_cast<uint64_t>(ptr[0]) << 56) |
           (static_cast<uint64_t>(ptr[1]) << 48) |
           (static_cast<uint64_t>(ptr[2]) << 40) |
           (static_cast<uint64_t>(ptr[3]) << 32) |
           (static_cast<uint64_t>(ptr[4]) << 24) |
           (static_cast<uint64_t>(ptr[5]) << 16) |
           (static_cast<uint64_t>(ptr[6]) << 8)  |
           static_cast<uint64_t>(ptr[7]);
}

inline void EncodeUint32(char* dst, uint32_t val) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    ptr[0] = static_cast<uint8_t>((val >> 24) & 0xFF);
    ptr[1] = static_cast<uint8_t>((val >> 16) & 0xFF);
    ptr[2] = static_cast<uint8_t>((val >> 8)  & 0xFF);
    ptr[3] = static_cast<uint8_t>(val & 0xFF);
}

inline uint32_t DecodeUint32(const char* src) {
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(src);
    return (static_cast<uint32_t>(ptr[0]) << 24) |
           (static_cast<uint32_t>(ptr[1]) << 16) |
           (static_cast<uint32_t>(ptr[2]) << 8)  |
           static_cast<uint32_t>(ptr[3]);
}

// 1. Raft 业务操作码定义 (Opcode)
enum RaftOpcode : uint8_t {
    kRequestVote          = 0x01,  // 正式选票请求 Args
    kRequestVoteReply     = 0x02,  // 正式选票响应 Reply
    kAppendEntries        = 0x03,  // 日志同步 / 心跳 Args
    kAppendEntriesReply   = 0x04,  // 日志同步响应 Reply
    kInstallSnapshot      = 0x05,  // 流式安装快照 Args
    kInstallSnapshotReply = 0x06,  // 流式安装快照 Reply
    kPreVote              = 0x07,  // 预投票试探 Args
    kPreVoteReply         = 0x08,  // 预投票试探响应 Reply (修正命名)
    kReadIndex            = 0x09,  // 线性一致读 Args (预留)
    kReadIndexReply       = 0x0A   // 线性一致读 Reply (预留)
};

enum class EntryType : uint8_t {
    kNormal = 0,     
    kConfiguration = 1
};

// 2. 日志条目定义
struct LogEntry {
    uint64_t index{0};                  // 日志物理索引
    uint64_t term{0};                   // Leader 任期
    EntryType type{EntryType::kNormal}; // 条目类型
    std::string data;                   // 业务载荷

    std::string Serialize() const {
        uint32_t body_len = static_cast<uint32_t>(data.size());
        std::string buf;
        buf.resize(8 + 8 + 1 + 4 + body_len); // 21 字节 Header + 载荷

        char* ptr = &buf[0];
        EncodeUint64(ptr, index); ptr += 8;
        EncodeUint64(ptr, term);  ptr += 8;
        *reinterpret_cast<uint8_t*>(ptr) = static_cast<uint8_t>(type); ptr += 1;
        EncodeUint32(ptr, body_len); ptr += 4;
        
        if (body_len > 0) {
            std::memcpy(ptr, data.data(), body_len);
        }
        return buf;
    }

    static LogEntry Deserialize(std::string_view src) {
        LogEntry entry{0, 0, EntryType::kNormal, ""};
        if (src.size() < 21) return entry;

        const char* ptr = src.data();
        entry.index = DecodeUint64(ptr); ptr += 8;
        entry.term  = DecodeUint64(ptr); ptr += 8;
        entry.type  = static_cast<EntryType>(*reinterpret_cast<const uint8_t*>(ptr)); ptr += 1;
        uint32_t body_len = DecodeUint32(ptr); ptr += 4;

        if (body_len > 0 && (src.size() >= 21 + body_len)) {
            entry.data.assign(ptr, body_len);
        }
        return entry;
    }
};

// 3. Raft 选票请求与响应结构体 (含 Pre-Vote 复用别名)
struct RequestVoteArgs {
    uint64_t term{0};          // 候选人的任期 (或 PreVote 试探的 term + 1)
    uint32_t candidate_id{0};  // 候选人节点 ID
    uint64_t last_log_index{0};// 候选人最后一条日志索引
    uint64_t last_log_term{0}; // 候选人最后一条日志任期

    std::string Serialize() const {
        std::string res;
        res.resize(28); // 8 + 4 + 8 + 8 = 28 字节
        char* ptr = &res[0];
        EncodeUint64(ptr, term);           ptr += 8;
        EncodeUint32(ptr, candidate_id);   ptr += 4;
        EncodeUint64(ptr, last_log_index); ptr += 8;
        EncodeUint64(ptr, last_log_term);
        return res;
    }

    static RequestVoteArgs Deserialize(std::string_view src) {
        RequestVoteArgs args{0, 0, 0, 0};
        if (src.size() < 28) return args;
        const char* ptr = src.data();
        args.term           = DecodeUint64(ptr); ptr += 8;
        args.candidate_id   = DecodeUint32(ptr); ptr += 4;
        args.last_log_index = DecodeUint64(ptr); ptr += 8;
        args.last_log_term  = DecodeUint64(ptr);
        return args;
    }
};

struct RequestVoteReply {
    uint64_t term{0};         // 当前任期
    bool vote_granted{false}; // 是否同意给票 / 准入
    uint64_t voter_id{0};     // 投票节点 ID

    std::string Serialize() const {
        std::string res;
        res.resize(17); // 8 + 1 + 8 = 17 字节
        char* ptr = &res[0];
        EncodeUint64(ptr, term); ptr += 8;
        *reinterpret_cast<uint8_t*>(ptr) = vote_granted ? 1 : 0; ptr += 1;
        EncodeUint64(ptr, voter_id);
        return res;
    }

    static RequestVoteReply Deserialize(std::string_view src) {
        RequestVoteReply reply{0, false, 0};
        if (src.size() < 17) return reply;
        const char* ptr = src.data();
        reply.term         = DecodeUint64(ptr); ptr += 8;
        reply.vote_granted = (*reinterpret_cast<const uint8_t*>(ptr) == 1); ptr += 1;
        reply.voter_id     = DecodeUint64(ptr);
        return reply;
    }
};

using PreVoteArgs = RequestVoteArgs;
using PreVoteReply = RequestVoteReply;


// 4.AppendEntries 日志同步结构体
struct AppendEntriesArgs {
    uint64_t term{0}; 
    uint64_t leader_id{0}; 
    uint64_t prev_log_index{0}; 
    uint64_t prev_log_term{0}; 
    uint64_t leader_commit{0}; 
    std::string entries; 

    std::string Serialize() const {
        uint32_t entries_len = static_cast<uint32_t>(entries.size());
        std::string res;
        res.resize(44 + entries_len); // 8*5 + 4 = 44 字节 Header + 载荷
        char* ptr = &res[0];
        
        EncodeUint64(ptr, term);           ptr += 8;
        EncodeUint64(ptr, leader_id);      ptr += 8;
        EncodeUint64(ptr, prev_log_index); ptr += 8;
        EncodeUint64(ptr, prev_log_term);  ptr += 8;
        EncodeUint64(ptr, leader_commit);  ptr += 8;
        EncodeUint32(ptr, entries_len);    ptr += 4;
        
        if (entries_len > 0) {
            std::memcpy(ptr, entries.data(), entries_len);
        }
        return res;
    }

    static AppendEntriesArgs Deserialize(std::string_view src) {
        AppendEntriesArgs args{0, 0, 0, 0, 0, ""};
        if (src.size() < 44) return args;
        const char* ptr = src.data();
        
        args.term           = DecodeUint64(ptr); ptr += 8;
        args.leader_id      = DecodeUint64(ptr); ptr += 8;
        args.prev_log_index = DecodeUint64(ptr); ptr += 8;
        args.prev_log_term  = DecodeUint64(ptr); ptr += 8;
        args.leader_commit  = DecodeUint64(ptr); ptr += 8;
        
        uint32_t entries_len = DecodeUint32(ptr); ptr += 4;
        if (entries_len > 0 && (src.size() >= 44 + entries_len)) {
            args.entries.assign(ptr, entries_len);
        }
        return args;
    }
};

struct AppendEntriesReply {
    uint64_t term{0};            // Follower 当前任期
    uint64_t success{0};         // 是否成功接收 (1=true, 0=false)
    uint64_t conflict_index{0};  // 冲突日志首个索引
    uint64_t conflict_term{0};   // 冲突日志任期
    uint64_t follower_id{0};     // Follower 节点 ID
    uint64_t match_index{0};     // Follower 实际追加落盘的日志水位线

    std::string Serialize() const {
        std::string res;
        res.resize(48); // 6 * 8 = 48 字节
        char* ptr = &res[0];
        EncodeUint64(ptr, term);           ptr += 8;
        EncodeUint64(ptr, success);        ptr += 8;
        EncodeUint64(ptr, conflict_index); ptr += 8;
        EncodeUint64(ptr, conflict_term);  ptr += 8;
        EncodeUint64(ptr, follower_id);    ptr += 8;
        EncodeUint64(ptr, match_index);
        return res;
    }

    static AppendEntriesReply Deserialize(std::string_view src) {
        AppendEntriesReply reply{0, 0, 0, 0, 0, 0};
        if (src.size() < 48) return reply;
        const char* ptr = src.data();
        reply.term           = DecodeUint64(ptr); ptr += 8;
        reply.success        = DecodeUint64(ptr); ptr += 8;
        reply.conflict_index = DecodeUint64(ptr); ptr += 8;
        reply.conflict_term  = DecodeUint64(ptr); ptr += 8;
        reply.follower_id    = DecodeUint64(ptr); ptr += 8;
        reply.match_index    = DecodeUint64(ptr);
        return reply;
    }
};

// 5. 流式安装快照协议包 (InstallSnapshot)
struct InstallSnapshotArgs {
    uint64_t term{0};                 // Leader 当前任期
    uint64_t leader_id{0};            // Leader 节点 ID
    uint64_t last_included_index{0};  // 快照中最后一条日志 Index
    uint64_t last_included_term{0};   // 快照中最后一条日志 Term
    std::string snapshot_filepath;    // 快照文件物理存根路径

    std::string Serialize() const {
        uint32_t path_len = static_cast<uint32_t>(snapshot_filepath.size());
        std::string res;
        res.resize(36 + path_len); // 8*4 + 4 = 36 字节 Header + 路径长度
        char* ptr = &res[0];
        
        EncodeUint64(ptr, term);                ptr += 8;
        EncodeUint64(ptr, leader_id);           ptr += 8;
        EncodeUint64(ptr, last_included_index); ptr += 8;
        EncodeUint64(ptr, last_included_term);  ptr += 8;
        EncodeUint32(ptr, path_len);            ptr += 4;
        
        if (path_len > 0) {
            std::memcpy(ptr, snapshot_filepath.data(), path_len);
        }
        return res;
    }

    static InstallSnapshotArgs Deserialize(std::string_view src) {
        InstallSnapshotArgs args{0, 0, 0, 0, ""};
        if (src.size() < 36) return args;
        const char* ptr = src.data();
        
        args.term                = DecodeUint64(ptr); ptr += 8;
        args.leader_id           = DecodeUint64(ptr); ptr += 8;
        args.last_included_index = DecodeUint64(ptr); ptr += 8;
        args.last_included_term  = DecodeUint64(ptr); ptr += 8;
        
        uint32_t path_len = DecodeUint32(ptr); ptr += 4;
        if (path_len > 0 && (src.size() >= 36 + path_len)) {
            args.snapshot_filepath.assign(ptr, path_len);
        }
        return args;
    }
};

struct InstallSnapshotReply {
    uint64_t term{0};        // Follower 当前任期
    uint64_t follower_id{0}; // 接收快照的 Follower ID
    uint64_t success{0};     // 快照是否成功挂载 (1=true, 0=false)

    std::string Serialize() const {
        std::string res;
        res.resize(24); // 3 * 8 = 24 字节
        char* ptr = &res[0];
        EncodeUint64(ptr, term);        ptr += 8;
        EncodeUint64(ptr, follower_id);  ptr += 8;
        EncodeUint64(ptr, success);
        return res;
    }

    static InstallSnapshotReply Deserialize(std::string_view src) {
        InstallSnapshotReply reply{0, 0, 0};
        if (src.size() < 24) return reply;
        const char* ptr = src.data();
        reply.term        = DecodeUint64(ptr); ptr += 8;
        reply.follower_id = DecodeUint64(ptr); ptr += 8;
        reply.success     = DecodeUint64(ptr);
        return reply;
    }
};

// 6. 批量日志条目网络打包与拆包工具函数
inline std::string SerializeEntries(const std::vector<LogEntry>& entries) {
    std::string res;
    uint32_t count = static_cast<uint32_t>(entries.size());
    char count_buf[4];
    EncodeUint32(count_buf, count);
    res.append(count_buf, 4);
    
    for (const auto& entry : entries) {
        std::string entry_bin = entry.Serialize();
        uint32_t entry_size = static_cast<uint32_t>(entry_bin.size());
        char size_buf[4];
        EncodeUint32(size_buf, entry_size);
        res.append(size_buf, 4);
        res.append(entry_bin);
    }
    return res;
}

inline std::vector<LogEntry> DeserializeEntries(std::string_view src) {
    std::vector<LogEntry> entries;
    if (src.size() < 4) return entries;
    
    const char* ptr = src.data();
    const char* end = src.data() + src.size();
    
    uint32_t count = DecodeUint32(ptr);
    ptr += 4;

    if (count > 0 && count <= 100000) {
        entries.reserve(count);
    }
    
    for (uint32_t i = 0; i < count; ++i) {
        if (static_cast<size_t>(end - ptr) < 4) break;
        uint32_t entry_size = DecodeUint32(ptr);
        ptr += 4;
        
        if (static_cast<size_t>(end - ptr) < entry_size) break;
        std::string_view entry_view(ptr, entry_size);
        entries.push_back(LogEntry::Deserialize(entry_view));
        ptr += entry_size;
    }
    return entries;
}

} // namespace raft_rpc