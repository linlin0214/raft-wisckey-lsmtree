#pragma once
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
    GET_RAW            = 0x05   
};

#pragma pack(push, 1)
struct WireCommandHeader {
    Opcode   opcode;   // 1 字节 操作码
    int32_t  key;      // 4 字节 键
    uint32_t val_len;  // 4 字节 值长度
};
#pragma pack(pop)

class WireProtocol {
public:
    static constexpr size_t   kHeaderSize = sizeof(WireCommandHeader); // 刚性 9 字节
    static constexpr uint32_t kMaxValLen  = 64 * 1024 * 1024;          // 64MB 上限

    // 二进制零拷贝序列化（大端网络字节序）
    static std::string Serialize(Opcode op, int32_t key, std::string_view value) {
        std::string packet;
        packet.resize(kHeaderSize + value.size());
        uint8_t* ptr = reinterpret_cast<uint8_t*>(&packet[0]);

        ptr[0] = static_cast<uint8_t>(op);

        uint32_t ukey = static_cast<uint32_t>(key);
        ptr[1] = static_cast<uint8_t>((ukey >> 24) & 0xFF);
        ptr[2] = static_cast<uint8_t>((ukey >> 16) & 0xFF);
        ptr[3] = static_cast<uint8_t>((ukey >> 8)  & 0xFF);
        ptr[4] = static_cast<uint8_t>(ukey & 0xFF);

        uint32_t vlen = static_cast<uint32_t>(value.size());
        ptr[5] = static_cast<uint8_t>((vlen >> 24) & 0xFF);
        ptr[6] = static_cast<uint8_t>((vlen >> 16) & 0xFF);
        ptr[7] = static_cast<uint8_t>((vlen >> 8)  & 0xFF);
        ptr[8] = static_cast<uint8_t>(vlen & 0xFF);

        if (!value.empty()) {
            std::memcpy(ptr + kHeaderSize, value.data(), value.size());
        }

        return packet;
    }

    // 🚀 修复点 1：增加仅解析 9 字节帧头的接口（供客户端分步读取使用）
    static std::optional<WireCommandHeader> ParseHeader(std::string_view raw_packet) {
        if (raw_packet.size() < kHeaderSize) {
            return std::nullopt;
        }

        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(raw_packet.data());

        Opcode op = static_cast<Opcode>(ptr[0]);

        uint32_t ukey = (static_cast<uint32_t>(ptr[1]) << 24) |
                        (static_cast<uint32_t>(ptr[2]) << 16) |
                        (static_cast<uint32_t>(ptr[3]) << 8)  |
                        static_cast<uint32_t>(ptr[4]);
        int32_t key = static_cast<int32_t>(ukey);

        uint32_t vlen = (static_cast<uint32_t>(ptr[5]) << 24) |
                        (static_cast<uint32_t>(ptr[6]) << 16) |
                        (static_cast<uint32_t>(ptr[7]) << 8)  |
                        static_cast<uint32_t>(ptr[8]);

        if (vlen > kMaxValLen) {
            spdlog::error("[WireProtocol] 解析捕获异常 ValueLen: {} (超出 64MB 防爆门限)！", vlen);
            return std::nullopt;
        }

        return WireCommandHeader{op, key, vlen};
    }

    // 🚀 修复点 2：全包解析接口复用 ParseHeader
    static std::optional<std::tuple<WireCommandHeader, std::string_view>> Parse(std::string_view raw_packet) {
        auto header_opt = ParseHeader(raw_packet);
        if (!header_opt.has_value()) {
            return std::nullopt;
        }

        if (raw_packet.size() < kHeaderSize + header_opt->val_len) {
            return std::nullopt;
        }

        std::string_view value_view = raw_packet.substr(kHeaderSize, header_opt->val_len);
        return std::make_optional(std::make_tuple(*header_opt, value_view));
    }
};

} // namespace raft_node