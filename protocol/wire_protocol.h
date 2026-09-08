#pragma once

#include "lsmtree/src/Slice.h"
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <tuple>
#include <cstring>
#include <algorithm>

namespace raft_node {

// 1. 业务层操作码定义
enum class Opcode : uint8_t {
    PUT_RAW     = 0x01,
    GET_RAW     = 0x02,
    DEL         = 0x03,
    PUT_META    = 0x04,
    GET_META    = 0x05,
    WRITE_BATCH = 0x06
};

// 重载运算符，防止已有代码中 uint8_t 与 Opcode 混比导致编译中断
inline constexpr bool operator==(uint8_t lhs, Opcode rhs) noexcept {
    return lhs == static_cast<uint8_t>(rhs);
}
inline constexpr bool operator==(Opcode lhs, uint8_t rhs) noexcept {
    return static_cast<uint8_t>(lhs) == rhs;
}
inline constexpr bool operator!=(uint8_t lhs, Opcode rhs) noexcept {
    return !(lhs == rhs);
}
inline constexpr bool operator!=(Opcode lhs, uint8_t rhs) noexcept {
    return !(lhs == rhs);
}

// 2. 刚性 16 字节网络帧头 (opcode 字段类型直接对齐为 Opcode)
#pragma pack(push, 1)
struct WireCommandHeader {
    uint16_t magic;    // 0x5750 ("WP")
    Opcode   opcode;   // 操作类型 (严格 1 字节)
    uint8_t  flags;    // 预留位 (1 字节)
    uint32_t body_len; // 载荷长度 (4 字节)
    uint64_t req_id;   // 请求 ID (8 字节)
};
#pragma pack(pop)

static_assert(sizeof(WireCommandHeader) == 16, "WireCommandHeader 必须严格为 16 字节！");

// 3. 批处理子项结构
struct BatchItem {
    Opcode opcode;
    std::string key;
    std::string value;
};

class WireProtocol {
public:
    static constexpr uint16_t kMagicNumber = 0x5750;
    static constexpr size_t   kHeaderSize  = sizeof(WireCommandHeader); // 16 字节
    static constexpr uint32_t kMaxBodySize = 64 * 1024 * 1024;          // 64MB 单包防爆

    // 序列化单条命令
    static std::string Serialize(Opcode op, const Slice& key, std::string_view val, uint64_t req_id) {
        uint16_t klen = static_cast<uint16_t>(key.size());
        uint32_t vlen = static_cast<uint32_t>(val.size());
        uint32_t body_len = sizeof(klen) + klen + vlen;

        std::string buf;
        buf.resize(kHeaderSize + body_len);
        char* ptr = buf.data();

        WireCommandHeader hdr{kMagicNumber, op, 0, body_len, req_id};
        std::memcpy(ptr, &hdr, kHeaderSize);
        ptr += kHeaderSize;

        std::memcpy(ptr, &klen, sizeof(klen));
        ptr += sizeof(klen);

        if (klen > 0) {
            std::memcpy(ptr, key.data(), klen);
            ptr += klen;
        }

        if (vlen > 0) {
            std::memcpy(ptr, val.data(), vlen);
        }

        return buf;
    }

    // 序列化批处理命令
    static std::string SerializeBatch(const std::vector<BatchItem>& items, uint64_t req_id) {
        uint32_t count = static_cast<uint32_t>(items.size());
        uint32_t payload_size = sizeof(count);
        
        for (const auto& item : items) {
            payload_size += sizeof(uint8_t) + sizeof(uint16_t) + item.key.size() + sizeof(uint32_t) + item.value.size();
        }

        std::string buf;
        buf.resize(kHeaderSize + payload_size);
        char* ptr = buf.data();

        WireCommandHeader hdr{kMagicNumber, Opcode::WRITE_BATCH, 0, payload_size, req_id};
        std::memcpy(ptr, &hdr, kHeaderSize);
        ptr += kHeaderSize;

        std::memcpy(ptr, &count, sizeof(count));
        ptr += sizeof(count);

        for (const auto& item : items) {
            uint8_t op = static_cast<uint8_t>(item.opcode);
            uint16_t klen = static_cast<uint16_t>(item.key.size());
            uint32_t vlen = static_cast<uint32_t>(item.value.size());

            std::memcpy(ptr, &op, sizeof(op));
            ptr += sizeof(op);

            std::memcpy(ptr, &klen, sizeof(klen));
            ptr += sizeof(klen);
            if (klen > 0) {
                std::memcpy(ptr, item.key.data(), klen);
                ptr += klen;
            }

            std::memcpy(ptr, &vlen, sizeof(vlen));
            ptr += sizeof(vlen);
            if (vlen > 0) {
                std::memcpy(ptr, item.value.data(), vlen);
                ptr += vlen;
            }
        }

        return buf;
    }

    // 解包批处理 Payload
    static std::vector<BatchItem> ParseBatch(std::string_view body) {
        std::vector<BatchItem> items;
        if (body.size() < sizeof(uint32_t)) {
            return items;
        }

        const char* ptr = body.data();
        const char* end = ptr + body.size();

        uint32_t count = 0;
        std::memcpy(&count, ptr, sizeof(count));
        ptr += sizeof(count);

        if (count > 0 && count <= 100000) {
            items.reserve(count);
        }

        for (uint32_t i = 0; i < count && ptr < end; ++i) {
            if (static_cast<size_t>(end - ptr) < sizeof(uint8_t) + sizeof(uint16_t)) {
                break;
            }

            uint8_t op = *reinterpret_cast<const uint8_t*>(ptr);
            ptr += sizeof(uint8_t);

            uint16_t klen = 0;
            std::memcpy(&klen, ptr, sizeof(klen));
            ptr += sizeof(klen);

            if (static_cast<size_t>(end - ptr) < klen + sizeof(uint32_t)) {
                break;
            }

            std::string key(ptr, klen);
            ptr += klen;

            uint32_t vlen = 0;
            std::memcpy(&vlen, ptr, sizeof(vlen));
            ptr += sizeof(vlen);

            if (static_cast<size_t>(end - ptr) < vlen) {
                break;
            }

            std::string val(ptr, vlen);
            ptr += vlen;

            items.push_back(BatchItem{static_cast<Opcode>(op), std::move(key), std::move(val)});
        }

        return items;
    }

    // 探针滑动定位合法魔数
    static std::optional<std::pair<WireCommandHeader, size_t>> ParseHeaderWithProbe(std::string_view stream) {
        if (stream.size() < kHeaderSize) {
            return std::nullopt;
        }

        size_t skip = 0;
        while (skip + 1 < stream.size()) {
            uint16_t magic = 0;
            std::memcpy(&magic, stream.data() + skip, sizeof(magic));
            if (magic == kMagicNumber) {
                break;
            }
            skip++;
        }

        if (stream.size() - skip < kHeaderSize) {
            return std::nullopt;
        }

        WireCommandHeader hdr;
        std::memcpy(&hdr, stream.data() + skip, kHeaderSize);

        if (hdr.body_len > kMaxBodySize) {
            return std::nullopt;
        }

        return std::make_pair(hdr, skip);
    }

    // 解析全包
    static std::optional<std::tuple<WireCommandHeader, std::string_view, std::string_view, size_t>> Parse(std::string_view stream) {
        auto probe = ParseHeaderWithProbe(stream);
        if (!probe.has_value()) {
            return std::nullopt;
        }

        auto [hdr, skip] = *probe;

        if (stream.size() < skip + kHeaderSize + hdr.body_len) {
            return std::nullopt;
        }

        std::string_view body(stream.data() + skip + kHeaderSize, hdr.body_len);

        if (hdr.opcode == Opcode::WRITE_BATCH) {
            return std::make_tuple(hdr, std::string_view(""), body, skip);
        }

        if (body.size() < sizeof(uint16_t)) {
            return std::nullopt;
        }

        uint16_t klen = 0;
        std::memcpy(&klen, body.data(), sizeof(klen));
        if (body.size() < sizeof(uint16_t) + klen) {
            return std::nullopt;
        }

        std::string_view key_view(body.data() + sizeof(uint16_t), klen);
        std::string_view val_view(body.data() + sizeof(uint16_t) + klen, body.size() - sizeof(uint16_t) - klen);

        return std::make_tuple(hdr, key_view, val_view, skip);
    }
};

} // namespace raft_node