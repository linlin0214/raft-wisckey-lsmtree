#pragma once
#include <string>
#include <cstring>
#include <cstdint>

// 代表数据在 vLog 文件中的物理绝对坐标指针 (在 64 位 Linux 下占用 16 字节，极其轻量)
struct VLogPointer {
    uint32_t file_id;  // 物理 Segment 文件编号
    uint64_t offset;   // 文件的绝对物理偏移量
    uint32_t size;     // 真实 Value 的字节大小

    //  将结构体二进制无损序列化为 16 字节 string，准备塞入内存 MemTable 和 SSTable
    std::string Encode() const {
        std::string buf;
        buf.resize(sizeof(VLogPointer));
        std::memcpy(buf.data(), this, sizeof(VLogPointer));
        return buf;
    }

    //  反序列化接口
    static VLogPointer Decode(const std::string& buf) {
        VLogPointer ptr;
        if (buf.size() == sizeof(VLogPointer)) {
            std::memcpy(&ptr, buf.data(), sizeof(VLogPointer));
        } else {
            ptr = {0, 0, 0}; // 异常占位符或 Tombstone 墓碑
        }
        return ptr;
    }
};