#pragma once
#include <unordered_map>
#include <list>
#include <string>
#include <mutex>
#include <vector>
#include <memory>

class LRUCache {
public:
    LRUCache(size_t capacity) : capacity_(capacity) {}
    
    //  显式传递非 const 引用获取缓存值，避免发生多余的字符串拷贝开销
    bool Get(const std::string& key, std::string& value) {
        std::lock_guard<std::mutex> lock(mtx_);

        auto it = map_.find(key);
        if (it != map_.end()) {
            value = it->second->value;
            //  调座位：将最新被访问的节点物理移动至双向链表的头部（最常访问区）
            list_.splice(list_.begin(), list_, it->second);
            return true;
        }
        return false;
    }

    void Put(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(mtx_);

        auto it = map_.find(key);
        
        if (it != map_.end()) {
            //  严格分支 1：如果是历史旧键覆写，仅修改其 Value，并移动至链表头，在此处被 else 彻底隔离！
            it->second->value = value;
            list_.splice(list_.begin(), list_, it->second);
        } else {
            //  严格分支 2：全新键值对插入
            // 先验证当前链表元素是否已达到或超越设定的最大容量限额
            if (list_.size() >= capacity_) {
                //  核心纠偏：淘汰时，必须提取链表最末尾节点的 .key 钥匙，才能准确从哈希表中移除映射！
                std::string oldest_key = list_.back().key; 
                map_.erase(oldest_key);   // 安全擦除哈希索引，杜绝野指针残留
                list_.pop_back();         // 物理释放双向链表尾部节点内存
            }
            //  将全新的 CacheNode 压入链表最前端
            list_.push_front({key, value});
            //  将其完美的双向链表首节点迭代器记录进哈希表，完成 O(1) 索引闭环
            map_[key] = list_.begin();
        }
    }

    size_t Size() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return list_.size();
    }

private:
    struct CacheNode {
        std::string key;
        std::string value;
    };
    size_t capacity_;
    std::list<CacheNode> list_;
    std::unordered_map<std::string, std::list<CacheNode>::iterator> map_;
    mutable std::mutex mtx_;
};

//  高并发分片缓存（Sharded LRU）：彻底分散全局锁竞争，极大提升 Linux 多线程读取吞吐
class ShardedLRUCache {
public:
    ShardedLRUCache(size_t total_capacity, size_t num_shards) {
        size_t per_shard_capacity = total_capacity / num_shards;
        for (size_t i = 0; i < num_shards; i++) {
            shards_.push_back(std::make_shared<LRUCache>(per_shard_capacity));
        }
    }

    bool Get(const std::string& key, std::string& value) {
        //  计算哈希槽位，精准定位并落入专属的缓存分片盒子里
        size_t shard_id = std::hash<std::string>{}(key) % shards_.size(); 
        return shards_[shard_id]->Get(key, value);
    }

    void Put(const std::string& key, const std::string& value) {
        size_t shard_id = std::hash<std::string>{}(key) % shards_.size();
        shards_[shard_id]->Put(key, value);
    }

    size_t Size() const {
        size_t total_size = 0;
        for (size_t i = 0; i < shards_.size(); i++) {
            total_size += shards_[i]->Size();
        }
        return total_size;
    }

private:
    std::vector<std::shared_ptr<LRUCache>> shards_; //  使用安全的智能指针平铺分片
};