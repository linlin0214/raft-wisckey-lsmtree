#pragma once
#include "Slice.h"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <vector>
#include <string>
#include <cstdint>
#include <immintrin.h>//PAUSE 汇编指令头文件

class ThreadWrite{
    public:
        enum state : uint8_t{
            STATE_INIT = 0,
            STATE_LEADER = 1,
            STATE_FOLLOWER = 2,
            STATE_WATTING = 3,
            STATE_COMPLETE = 4
        };
        //代表前台并发线程的单次写入请求包
        struct Writer {
            std::string key;
            const std::string* value;
            std::atomic<uint8_t> state{0};
            Writer* next{nullptr};
            std::mutex mutex_;
            std::condition_variable cv_;
            Writer(const Slice& k, const std::string* v) : key(k.ToString()), value(v) {}
        };
        
        //  wal组提交使用的批量数据载体
        struct WriteBatch {
            struct Entry {
                std::string key;
                std::string value;
            };
            std::vector<Entry> entries;

            void Clear() { entries.clear(); }
            size_t Count() const { return entries.size(); }
        };
        
        
        void JoinBatchGroup(Writer* writer){
            //取出栈顶元素
            Writer* old_head = newest_writerr_.load(std::memory_order_relaxed);
            //CAS压栈
            while(true){
                writer->next = old_head;
                // 尝试把 newest_writerr_ 从 old_head 改为 writer
                if(newest_writerr_.compare_exchange_weak(
                    old_head,writer,std::memory_order_release,std::memory_order_relaxed
                )){
                    break;
                }
            }
            if(old_head == nullptr){
                    writer->state.store(STATE_LEADER,std::memory_order_relaxed);
                }else{
                    writer->state.store(STATE_FOLLOWER,std::memory_order_relaxed);
                }

        }

        void AwaitState(Writer* writer,int target_state){
            //1. 阶段 1：自旋轮询（Spinning）
            for(int spin = 0;spin<200;spin++){
                //自旋期间state被leader改变成功
                if(writer->state.load(std::memory_order_acquire) == target_state){
                    return ;
                }
                _mm_pause();//优化 CPU 流水线
            }
            // 2. 阶段 2：自旋超时，进入操作系统的沉睡挂起
            //锁是必要的，用来确保writer在休眠前的最终状态
            std::unique_lock<std::mutex> lock(writer->mutex_);

            if (writer->state.load(std::memory_order_acquire) == target_state) {
                return;
            }
            writer->state.store(STATE_WATTING,std::memory_order_release);

            writer->cv_.wait(lock,[&](){
                return writer->state.load(std::memory_order_acquire)==target_state;
            });

        }

        void EnterAsBatchGroupLeader(std::vector<Writer*>& batch_group){
            //原子操作出队
            Writer* head = newest_writerr_.exchange(nullptr,std::memory_order_acquire);
            Writer* prev = nullptr;
            Writer* cur = head;
            //反转链表
            while(cur){
                Writer* next = cur->next;
                cur->next = prev;
                prev = cur;
                cur = next;
            }
            cur = prev;
            //不截断，全装进去
            while(cur){
                batch_group.push_back(cur);
                cur = cur->next;
            }
        }
        //唤醒本批次的所有 Follower
        void ExitAsBatchGroupLeader(std::vector<Writer*>& batch_size){
            for(auto& w : batch_size){
                if(w->state.load(std::memory_order_relaxed) == STATE_LEADER) continue;
                uint8_t old_state = w->state.exchange(STATE_COMPLETE,std::memory_order_release);
                if(old_state==STATE_WATTING){
                    std::unique_lock<std::mutex> lock(w->mutex_);
                    w->cv_.notify_one();
                }
            }
        }
    private:
        std::atomic<Writer*> newest_writerr_{nullptr};
};
