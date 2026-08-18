#pragma once
#include "Buffer.h"
#include <optional>
#include <string>
#include <string_view>
#include <cstdint>

namespace raft_rpc {

#pragma pack(push, 1)
struct RpcHeader {
    uint16_t magic;    // 魔数 (2字节)
    uint8_t  opcode;   // 操作码 (1字节)
    uint32_t body_len; // 业务载荷绝对长度 (4字节)
    uint64_t req_id;   // 业务 Request ID (8字节)
};
#pragma pack(pop)

struct RpcPacket {
    RpcHeader header;
    std::string body;
};

class Codec {
public:
    static constexpr uint16_t kMagicNumber = 0x4142;                // "AB"
    static constexpr size_t   kHeaderSize  = sizeof(RpcHeader);     // 刚性 15 字节
    static constexpr uint32_t kMaxBodySize = 64 * 1024 * 1024;      // 64MB 上限

    static std::string Encode(uint8_t opcode, uint64_t req_id, std::string_view body);
    static void EncodeToBuffer(uint8_t opcode, uint64_t req_id, std::string_view body, Buffer* buf);
    static std::optional<RpcPacket> Parse(Buffer* buf);
};

} // namespace raft_rpc