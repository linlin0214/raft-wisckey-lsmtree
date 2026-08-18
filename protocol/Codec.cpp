#include "Codec.h"
#include <cstring>
#include <spdlog/spdlog.h>

namespace raft_rpc {

std::string Codec::Encode(uint8_t opcode, uint64_t req_id, std::string_view body) {
    std::string result;
    result.resize(kHeaderSize + body.size());
    
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&result[0]);

    // 1. Magic (2B) - 大端网络字节序
    ptr[0] = static_cast<uint8_t>((kMagicNumber >> 8) & 0xFF);
    ptr[1] = static_cast<uint8_t>(kMagicNumber & 0xFF);

    // 2. Opcode (1B)
    ptr[2] = opcode;

    // 3. Body Length (4B) - 大端网络字节序
    uint32_t body_len = static_cast<uint32_t>(body.size());
    ptr[3] = static_cast<uint8_t>((body_len >> 24) & 0xFF);
    ptr[4] = static_cast<uint8_t>((body_len >> 16) & 0xFF);
    ptr[5] = static_cast<uint8_t>((body_len >> 8) & 0xFF);
    ptr[6] = static_cast<uint8_t>(body_len & 0xFF);

    // 4. Request ID (8B / 64bit) - 大端网络字节序 (🚀 补齐 15 字节完全对齐)
    ptr[7]  = static_cast<uint8_t>((req_id >> 56) & 0xFF);
    ptr[8]  = static_cast<uint8_t>((req_id >> 48) & 0xFF);
    ptr[9]  = static_cast<uint8_t>((req_id >> 40) & 0xFF);
    ptr[10] = static_cast<uint8_t>((req_id >> 32) & 0xFF);
    ptr[11] = static_cast<uint8_t>((req_id >> 24) & 0xFF);
    ptr[12] = static_cast<uint8_t>((req_id >> 16) & 0xFF);
    ptr[13] = static_cast<uint8_t>((req_id >> 8) & 0xFF);
    ptr[14] = static_cast<uint8_t>(req_id & 0xFF);

    // 5. Body
    if (!body.empty()) {
        std::memcpy(ptr + kHeaderSize, body.data(), body.size());
    }

    return result;
}

void Codec::EncodeToBuffer(uint8_t opcode, uint64_t req_id, std::string_view body, Buffer* buf) {
    std::string encoded = Encode(opcode, req_id, body);
    buf->Append(encoded);
}

std::optional<RpcPacket> Codec::Parse(Buffer* buf) {
    // 1. 检查数据是否满足 15 字节固定 Header
    if (buf->ReadableBytes() < kHeaderSize) {
        return std::nullopt;
    }

    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(buf->Peek());

    // 2. 探针滑动：寻找合法 Magic (0x4142)
    size_t readable = buf->ReadableBytes();
    size_t skip = 0;
    while (skip + 1 < readable) {
        uint16_t m = (static_cast<uint16_t>(ptr[skip]) << 8) | static_cast<uint16_t>(ptr[skip + 1]);
        if (m == kMagicNumber) {
            break;
        }
        skip++;
    }

    // 若发生了错位字节，仅安全滑动跳过错位字节，保护后续流
    if (skip > 0) {
        spdlog::warn("[Codec] 侦测到 {} 字节魔数错位/杂接数据，安全滑动清理！", skip);
        buf->Retrieve(skip);
        if (buf->ReadableBytes() < kHeaderSize) {
            return std::nullopt;
        }
        ptr = reinterpret_cast<const uint8_t*>(buf->Peek());
    }

    uint16_t magic = (static_cast<uint16_t>(ptr[0]) << 8) | static_cast<uint16_t>(ptr[1]);

    // 3. 解包 Opcode (1B) & Body Length (4B)
    uint8_t opcode = ptr[2];
    uint32_t body_len = (static_cast<uint32_t>(ptr[3]) << 24) |
                         (static_cast<uint32_t>(ptr[4]) << 16) |
                         (static_cast<uint32_t>(ptr[5]) << 8)  |
                         static_cast<uint32_t>(ptr[6]);

    // 防爆安全检查
    if (body_len > kMaxBodySize) {
        spdlog::critical("[Codec] 捕获到超大异常 BodyLen: {} 字节，跳过首字节重新对齐！", body_len);
        buf->Retrieve(1);
        return std::nullopt;
    }

    // 4. 检查全包数据是否到齐
    if (buf->ReadableBytes() < kHeaderSize + body_len) {
        return std::nullopt; // 半包状态，等待后续网络数据到达
    }

    // 5. 解包 Request ID (8B / 64bit)
    uint64_t req_id = (static_cast<uint64_t>(ptr[7])  << 56) |
                      (static_cast<uint64_t>(ptr[8])  << 48) |
                      (static_cast<uint64_t>(ptr[9])  << 40) |
                      (static_cast<uint64_t>(ptr[10]) << 32) |
                      (static_cast<uint64_t>(ptr[11]) << 24) |
                      (static_cast<uint64_t>(ptr[12]) << 16) |
                      (static_cast<uint64_t>(ptr[13]) << 8)  |
                      static_cast<uint64_t>(ptr[14]);

    // 6. 构建并填充 RpcPacket
    RpcPacket packet;
    packet.header.magic = magic;
    packet.header.opcode = opcode;
    packet.header.body_len = body_len;
    packet.header.req_id = req_id;

    if (body_len > 0) {
        packet.body.assign(reinterpret_cast<const char*>(ptr + kHeaderSize), body_len);
    }

    // 7. 从 Buffer 中消费已被解析的字节
    buf->Retrieve(kHeaderSize + body_len);

    return packet;
}

} // namespace raft_rpc