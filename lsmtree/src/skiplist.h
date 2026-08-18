#pragma once

#include <string>
#include <cstring> //  用于 memcpy
#include <cstdint>
#include "Arena.h"

//  链表节点定义
struct Node {
    int key;
    //  将 std::string 替换为原生 char* 指针和长度，防止内存逃逸至 heap，彻底杜绝系统内存碎片
    uint32_t val_len;
    char* val_ptr;
    
    //  彻底抛弃 struct 基础指针运算，采用绝对物理地址偏移 + void* 字节拷贝
    // 100% 斩断 GCC -O3 别名分析器的任何激进剪枝假设
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

    std::string GetValue() const {
        if (val_len == 0 || val_ptr == nullptr) return "";
        return std::string(val_ptr, val_len);
    }

    //  height = level + 1 (例如 level 0 实际上有 1 层前向指针)
    static Node* NewNode(Arena* arena, int k, const std::string& v, int height) {
        //  Node 自身大小（16 字节）+ 所有的层高指针所需的额外空间
        size_t size = sizeof(Node) + sizeof(Node*) * height;
        char* mem = arena->Allocatealigned(size);

        //  完全不通过 OS 申请堆内存，直接把 Arena 给它的现成对齐地址 mem 拿来进行 placement new 构造
        Node* node = new(mem) Node(k);

        //  将字符串的真实载荷也存入 Arena，消除系统堆碎片 and 内存泄漏
        node->val_len = static_cast<uint32_t>(v.size());
        if (node->val_len > 0) {
            node->val_ptr = arena->Allocate(node->val_len);
            std::memcpy(node->val_ptr, v.data(), node->val_len);
        } else {
            node->val_ptr = nullptr;
        }

        //  将所有层级的前向指针安全初始化为 nullptr
        for (int i = 0; i < height; ++i) {
            node->set_forward(i, nullptr);
        }
        return node;
    }

    Node(int k) : key(k), val_len(0), val_ptr(nullptr) {}

    ~Node() = default;
    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;
};

class skiplist {
public:
    class Iterator {
    public:
        explicit Iterator(Node* node) : current_(node) {}
        bool Valid() const { return current_ != nullptr; }
        void Next() { current_ = current_->forward(0); }
        int key() const { return current_->key; }
        std::string value() const { return current_->GetValue(); }
    private:
        Node* current_;
    };

    Iterator Begin() {
        return Iterator(header->forward(0));
    }

    skiplist(int maxl, Arena* arena)
        : cur_level(0),max_level(maxl), arena_(arena)
    {
        // 安全升级：将哨兵头节点的 key 初始化为 INT_MIN，建立绝对的左侧安全护城河
        header = Node::NewNode(arena_, -2147483648, "", max_level);
    }

    ~skiplist() {}

    const Node* search(int target) const;
    void insert(int key, const std::string& s, int level);

    void StealFrom(const skiplist& other) {
        this->header = other.header;
        this->cur_level = other.cur_level;
    }

    void Clear() {
        cur_level = 0;
        header = Node::NewNode(arena_, -2147483648, "", max_level);
    }

    bool empty() const {
        return header->forward(0) == nullptr;
    }

    //  黄金重构：在每次循环决定晋升时重新迭代 seed！
    // 这能让跳表节点高度产生完美的几何分布，彻底恢复 $O(\log N)$ 的工业级结构性能
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

inline const Node* skiplist::search(int target) const {
    const Node* cur = header;
    for (int i = cur_level; i >= 0; --i) {
        while (cur->forward(i) != nullptr && cur->forward(i)->key < target) {
            cur = cur->forward(i);
        }
    }
    cur = cur->forward(0);
    if (cur != nullptr && cur->key == target) {
        return cur;
    } else {
        return nullptr;
    }
}

inline void skiplist::insert(int key, const std::string& value, int level) {
    if (level >= max_level) level = max_level - 1;

    Node* update[64];
    Node* cur = header;

    if (level > cur_level) {
        for (int i = cur_level + 1; i <= level; ++i) {
            update[i] = header;
        }
    }

    for (int i = cur_level; i >= 0; --i) {
        while (cur->forward(i) != nullptr && cur->forward(i)->key < key) {
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