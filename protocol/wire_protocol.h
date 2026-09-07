#pragma once
#include "lsmtree/src/Slice.h"
#include <cstdint>
#include <string>
#include <string_view>
#include <cstring>
#include <tuple>
#include <optional>
#include <spdlog/spdlog.h>

namespace raft_node {

enum class Opcode : uint8_t {
    PUT_RAW            = 0x01,  
    DEL                = 0x02,  
    SET_DISTRIBUTED_EC = 0x03,  
    PUT_META           = 0x04,  
    GET_RAW            = 0x05,
    WRITE_BATCH        = 0x06   
};

#pragma pack(push, 1)
struct WireCommandHeader {
    uint16_t magic;    // 0x5749 ("WI")
    uint8_t  version;  // 0x01
    Opcode   opcode;   // 操作码
    uint32_t body_len; // 载荷长度 (KeyLen[2] + Key + ValLen[4] + Val)
    uint64_t req_id;   // 请求序列号
};
#pragma pack(pop)

class WireProtocol {
public:
    static constexpr uint16_t kMagicNumber = 0x5749;
    static constexpr size_t   kHeaderSize  = sizeof(WireCommandHeader); // 16 字节
    static constexpr uint32_t kMaxBodySize = 64 * 1024 * 1024;          // 64MB

    // 二进制大端打包
    static std::string Serialize(Opcode op, const Slice& key, std::string_view value, uint64_t req_id = 0) {
        uint16_t klen = static_cast<uint16_t>(key.size());
        uint32_t vlen = static_cast<uint32_t>(value.size());
        uint32_t body_len = 2 + klen + 4 + vlen;

        std::string packet;
        packet.resize(kHeaderSize + body_len);
        uint8_t* ptr = reinterpret_cast<uint8_t*>(&packet[0]);

        // 1. Header (16B)
        ptr[0] = static_cast<uint8_t>((kMagicNumber >> 8) & 0xFF);
        ptr[1] = static_cast<uint8_t>(kMagicNumber & 0xFF);
        ptr[2] = 0x01; // Version
        ptr[3] = static_cast<uint8_t>(op);

        ptr[4] = static_cast<uint8_t>((body_len >> 24) & 0xFF);
        ptr[5] = static_cast<uint8_t>((body_len >> 16) & 0xFF);
        ptr[6] = static_cast<uint8_t>((body_len >> 8)  & 0xFF);
        ptr[7] = static_cast<uint8_t>(body_len & 0xFF);

        ptr[8]  = static_cast<uint8_t>((req_id >> 56) & 0xFF);
        ptr[9]  = static_cast<uint8_t>((req_id >> 48) & 0xFF);
        ptr[10] = static_cast<uint8_t>((req_id >> 40) & 0xFF);
        ptr[11] = static_cast<uint8_t>((req_id >> 32) & 0xFF);
        ptr[12] = static_cast<uint8_t>((req_id >> 24) & 0xFF);
        ptr[13] = static_cast<uint8_t>((req_id >> 16) & 0xFF);
        ptr[14] = static_cast<uint8_t>((req_id >> 8)  & 0xFF);
        ptr[15] = static_cast<uint8_t>(req_id & 0xFF);

        // 2. Payload (2B + Key + 4B + Val)
        uint8_t* body_ptr = ptr + kHeaderSize;
        body_ptr[0] = static_cast<uint8_t>((klen >> 8) & 0xFF);
        body_ptr[1] = static_cast<uint8_t>(klen & 0xFF);
        if (klen > 0) {
            std::memcpy(body_ptr + 2, key.data(), klen);
        }

        uint8_t* val_meta_ptr = body_ptr + 2 + klen;
        val_meta_ptr[0] = static_cast<uint8_t>((vlen >> 24) & 0xFF);
        val_meta_ptr[1] = static_cast<uint8_t>((vlen >> 16) & 0xFF);
        val_meta_ptr[2] = static_cast<uint8_t>((vlen >> 8)  & 0xFF);
        val_meta_ptr[3] = static_cast<uint8_t>(vlen & 0xFF);
        if (vlen > 0) {
            std::memcpy(val_meta_ptr + 4, value.data(), vlen);
        }

        return packet;
    }

    // 仅解析 16 字节帧头并实施魔数滑动探针容错
    static std::optional<std::pair<WireCommandHeader, size_t>> ParseHeaderWithProbe(std::string_view stream) {
        if (stream.size() < kHeaderSize) return std::nullopt;

        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(stream.data());
        size_t readable = stream.size();
        size_t skip = 0;

        while (skip + 1 < readable) {
            uint16_t m = (static_cast<uint16_t>(ptr[skip]) << 8) | static_cast<uint16_t>(ptr[skip + 1]);
            if (m == kMagicNumber) break;
            skip++;
        }

        if (skip > 0) {
            if (readable - skip < kHeaderSize) return std::nullopt;
            ptr += skip;
        }

        uint16_t magic   = (static_cast<uint16_t>(ptr[0]) << 8) | static_cast<uint16_t>(ptr[1]);
        uint8_t  version = ptr[2];
        Opcode   opcode  = static_cast<Opcode>(ptr[3]);

        uint32_t body_len = (static_cast<uint32_t>(ptr[4]) << 24) |
                            (static_cast<uint32_t>(ptr[5]) << 16) |
                            (static_cast<uint32_t>(ptr[6]) << 8)  |
                            static_cast<uint32_t>(ptr[7]);

        uint64_t req_id = (static_cast<uint64_t>(ptr[8])  << 56) |
                          (static_cast<uint64_t>(ptr[9])  << 48) |
                          (static_cast<uint64_t>(ptr[10]) << 40) |
                          (static_cast<uint64_t>(ptr[11]) << 32) |
                          (static_cast<uint64_t>(ptr[12]) << 24) |
                          (static_cast<uint64_t>(ptr[13]) << 16) |
                          (static_cast<uint64_t>(ptr[14]) << 8)  |
                          static_cast<uint64_t>(ptr[15]);

        if (body_len > kMaxBodySize) {
            return std::nullopt;
        }

        WireCommandHeader hdr{magic, version, opcode, body_len, req_id};
        return std::make_pair(hdr, skip);
    }

    // 全包解包
    static std::optional<std::tuple<WireCommandHeader, std::string_view, std::string_view, size_t>> Parse(std::string_view stream) {
        auto header_res = ParseHeaderWithProbe(stream);
        if (!header_res.has_value()) return std::nullopt;

        auto [header, skip] = *header_res;
        if (stream.size() < skip + kHeaderSize + header.body_len) {
            return std::nullopt; // 半包
        }

        if (header.body_len < 6) { // 至少包含 2B klen + 4B vlen
            return std::nullopt;
        }

        const uint8_t* body_ptr = reinterpret_cast<const uint8_t*>(stream.data() + skip + kHeaderSize);
        uint16_t klen = (static_cast<uint16_t>(body_ptr[0]) << 8) | static_cast<uint16_t>(body_ptr[1]);

        if (2 + klen + 4 > header.body_len) return std::nullopt;

        const uint8_t* val_meta = body_ptr + 2 + klen;
        uint32_t vlen = (static_cast<uint32_t>(val_meta[0]) << 24) |
                        (static_cast<uint32_t>(val_meta[1]) << 16) |
                        (static_cast<uint32_t>(val_meta[2]) << 8)  |
                        static_cast<uint32_t>(val_meta[3]);

        if (2 + klen + 4 + vlen != header.body_len) return std::nullopt;

        std::string_view key_view(reinterpret_cast<const char*>(body_ptr + 2), klen);
        std::string_view val_view(reinterpret_cast<const char*>(val_meta + 4), vlen);

        return std::make_optional(std::make_tuple(header, key_view, val_view, skip));
    }
};

} // namespace raft_node