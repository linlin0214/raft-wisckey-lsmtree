#pragma once

#include "Arena.h"
#include "Slice.h"
#include <string>
#include <string_view>
#include <cstring>
#include <cstdint>

//  链表节点定义
struct Node {
    uint16_t key_len;
    uint16_t height;
    uint32_t val_len;

    Node* forward(int level) const {
        uintptr_t base_addr = reinterpret_cast<uintptr_t>(this) + sizeof(Node);
        Node* ptr = nullptr;
        std::memcpy(&ptr, reinterpret_cast<const void*>(base_addr + level * sizeof(Node*)), sizeof(Node*));
        return ptr;
    }

    void set_forward(int level, Node* ptr) {
        uintptr_t base_addr = reinterpret_cast<uintptr_t>(this) + sizeof(Node);
        std::memcpy(reinterpret_cast<void*>(base_addr + level * sizeof(Node*)), &ptr, sizeof(Node*));
    }

    const char* key_data() const {
        uintptr_t payload_addr = reinterpret_cast<uintptr_t>(this) + sizeof(Node) + height * sizeof(Node*);
        return reinterpret_cast<const char*>(payload_addr);
    }

    const char* val_data() const {
        return key_data() + key_len;
    }

    Slice key() const {
        return Slice(key_data(), key_len);
    }

    std::string GetValue() const {
        if (val_len == 0) return "";
        return std::string(val_data(), val_len);
    }

    // 单次物理内存连续分配
    static Node* NewNode(Arena* arena, const Slice& k, std::string_view v, int height) {
        size_t node_base = sizeof(Node) + sizeof(Node*) * height;
        size_t total_size = node_base + k.size() + v.size();
        char* mem = arena->Allocatealigned(total_size);

        Node* node = reinterpret_cast<Node*>(mem);
        node->key_len = static_cast<uint16_t>(k.size());
        node->height = static_cast<uint16_t>(height);
        node->val_len = static_cast<uint32_t>(v.size());

        for (int i = 0; i < height; ++i) {
            node->set_forward(i, nullptr);
        }

        char* payload = mem + node_base;
        if (k.size() > 0) {
            std::memcpy(payload, k.data(), k.size());
        }
        if (v.size() > 0) {
            std::memcpy(payload + k.size(), v.data(), v.size());
        }

        return node;
    }
};

class skiplist {
public:
    class Iterator {
    public:
        explicit Iterator(Node* node) : current_(node) {}
        bool Valid() const { return current_ != nullptr; }
        void Next() { current_ = current_->forward(0); }
        Slice key() const { return current_->key(); }
        std::string value() const { return current_->GetValue(); }
    private:
        Node* current_;
    };

    Iterator Begin() const {
        return Iterator(header->forward(0));
    }

    skiplist(int maxl, Arena* arena)
        : cur_level(0), max_level(maxl), arena_(arena) {
        // 头节点 Sentinel 维持空 Slice，仅作为搜索锚点，其 Key 不参与实际对比
        header = Node::NewNode(arena_, Slice(""), "", max_level);
    }

    ~skiplist() = default;

    const Node* search(const Slice& target) const;
    void insert(const Slice& key, std::string_view value, int level);

    void StealFrom(const skiplist& other) {
        this->header = other.header;
        this->cur_level = other.cur_level;
    }

    void Clear() {
        cur_level = 0;
        header = Node::NewNode(arena_, Slice(""), "", max_level);
    }

    bool empty() const {
        return header->forward(0) == nullptr;
    }

    //  黄金重构：在每次循环决定晋升时重新迭代 seed
    // 这能让跳表节点高度产生完美的几何分布，恢复 O(\log N) 的工业级结构性能
    int RandomLevel() {
        thread_local uint32_t seed = 2463534242U;
        int lvl = 0;
        while (lvl < max_level - 1) {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            if ((seed % 100) < 50) {
                lvl++;
            } else {
                break;
            }
        }
        return lvl;
    }

private:
    Node* header;
    int cur_level;
    int max_level;
    Arena* arena_;
};

inline const Node* skiplist::search(const Slice& target) const {
    const Node* cur = header;
    for (int i = cur_level; i >= 0; --i) {
        while (cur->forward(i) != nullptr && cur->forward(i)->key().compare(target) < 0) {
            cur = cur->forward(i);
        }
    }
    cur = cur->forward(0);
    if (cur != nullptr && cur->key().compare(target) == 0) {
        return cur;
    }
    return nullptr;
}

inline void skiplist::insert(const Slice& key, std::string_view value, int level) {
    if (level >= max_level) level = max_level - 1;

    Node* update[64];
    Node* cur = header;

    if (level > cur_level) {
        for (int i = cur_level + 1; i <= level; ++i) {
            update[i] = header;
        }
    }

    for (int i = cur_level; i >= 0; --i) {
        while (cur->forward(i) != nullptr && cur->forward(i)->key().compare(key) < 0) {
            cur = cur->forward(i);
        }
        update[i] = cur;
    }

    int height = level + 1;
    Node* newnode = Node::NewNode(arena_, key, value, height);

    for (int i = 0; i <= level; ++i) {
        newnode->set_forward(i, update[i]->forward(i));
        update[i]->set_forward(i, newnode);
    }

    if (level > cur_level) {
        cur_level = level;
    }
}