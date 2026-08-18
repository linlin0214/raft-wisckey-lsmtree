#pragma once

#include "lsmtree/src/RaftStorageAdapter.h"
#include "RaftCore.h"
#include "protocol/RaftRpc.h"
#include "network/EventLoop.h"
#include "network/TimerQueue.h"
#include "network/Connection.h"
#include "network/RpcClient.h"

#include <cstddef>
#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <queue>

namespace raft_rpc {
class RaftDispatcher;
}

class RaftNode : public std::enable_shared_from_this<RaftNode> {
public:
    struct NodeStatus {
        uint32_t node_id;
        std::string role;
        uint64_t term;
        uint32_t leader_id;
        uint64_t last_log_index;
        uint64_t commit_index;
        uint64_t stabled_index;
        uint64_t last_applied;
    };
    NodeStatus GetStatus();
    struct Peer {
        uint32_t id;
        std::string ip;
        uint16_t port;
        std::shared_ptr<RpcClient> client;
        //默认构造
        std::weak_ptr<Connection> outbound_conn{};
        std::weak_ptr<Connection> inbound_conn{};

        std::shared_ptr<Connection> GetActiveConnection() const {
            // 尝试提升 outbound_conn
            if(auto conn = outbound_conn.lock()){
                if(conn -> IsConnected()){
                    return conn; //返回强引用
                }
            }
            // 尝试提升 inbound_conn
            if(auto conn = inbound_conn.lock()){
                if(conn -> IsConnected()){
                    return conn;
                }
            }
            return nullptr;
        }
    };

public:
    //  快照配置参数：适配 4KB 载荷与高 QPS 目标的高门限设计
    static constexpr uint64_t kSnapshotCountThreshold = 500000; // 50万条日志 (~2GB)
    static constexpr uint64_t kSnapshotIntervalSec   = 300;    // 300秒 (5分钟)

public:
    RaftNode(
        EventLoop* loop,
        uint32_t node_id,
        const std::vector<uint32_t>& peer_ids,
        raft_rpc::RaftDispatcher* dispatcher
    );

    ~RaftNode();

public:
    void Start();

    void AddPeer(
        uint32_t peer_id,
        const std::string& ip,
        uint16_t port
    );

    bool Propose(
        int32_t key,
        std::string_view value,
        const std::shared_ptr<Connection>& client
    );

    bool ProposeRead(
        int32_t key,
        const std::shared_ptr<Connection>& client
    );

public:
    void HandleAppendEntries(
        const std::shared_ptr<Connection>& conn,
        uint64_t req_id,
        const raft_rpc::AppendEntriesArgs& args
    );

    void HandleAppendEntriesReply(
        uint64_t req_id,
        const raft_rpc::AppendEntriesReply& reply
    );

    void HandleRequestVote(
        const std::shared_ptr<Connection>& conn,
        uint64_t req_id,
        const raft_rpc::RequestVoteArgs& args
    );

    void HandleRequestVoteReply(
        uint64_t req_id,
        const raft_rpc::RequestVoteReply& reply
    );

    void HandleInstallSnapshot(
        const std::shared_ptr<Connection>& conn,
        uint64_t req_id,
        const raft_rpc::InstallSnapshotArgs& args
    );

    void HandleInstallSnapshotReply(
        uint64_t req_id,
        const raft_rpc::InstallSnapshotReply& reply
    );

    uint32_t GetNodeId() const { return node_id_; }

    uint32_t GetLeaderId() {
        std::lock_guard<std::mutex> lock(core_mtx_);
        return core_ ? core_->GetLeaderId() : RaftCore::kNoLeader;
    }

private:
    void OnTick();
    void ScheduleCheckAndAdvance();
    void DoCheckAndAdvance();
    void CheckAndAdvance();
    void OnPersistFinished(Ready rd, AdvanceState state);
    void StorageThreadLoop(); // 内嵌的后台落盘线程函数

    //  快照驱动与检测函数
    void CheckAndTriggerSnapshot(uint64_t current_applied);
    void TriggerSnapshot(uint64_t compact_index);

    void SendOutboundMessage(const OutboundMessage& msg);
    void UpdateInboundConnection(uint32_t peer_id, const std::shared_ptr<Connection>& conn);

private:
    EventLoop* loop_;                       // 挂载的 EventLoop 核心控制面
    uint32_t node_id_;                      // 当前节点 ID
    raft_rpc::RaftDispatcher* dispatcher_;  // 网络协议路由分发器
    
    std::vector<Peer> peers_;               // Peer 节点大盘
    std::unique_ptr<RaftCore> core_;        // 内存共识大脑
    std::mutex core_mtx_;                   // 保护 core_ 的互斥锁

    RaftStorageAdapter storage_adapter_;    // 持久化存储与 LSM-Tree 适配器

    // 后台专职落盘线程与生产者-消费者队列
    std::thread storage_thread_;
    std::mutex storage_queue_mtx_;
    std::condition_variable storage_cv_;
    std::queue<Ready> storage_queue_;
    std::atomic<bool> storage_running_{false};

    std::atomic<bool> is_ready_scheduled_{false}; // 调度去重标志位
    std::atomic<bool> is_persisting_{false};      // 落盘顺序屏障
    std::atomic<bool> is_snapshotting_{false};    // 快照并发屏障

    // 快照水位与时间状态追踪
    uint64_t last_snapshot_index_{0};
    uint64_t last_snapshot_time_sec_{0};

    TimerQueue tick_timer_;                 // Raft 逻辑 Tick 定时器

    std::mutex client_mtx_;                 // 保护 Client ACK 映射表
    std::unordered_map<uint64_t, std::shared_ptr<Connection>> client_wait_list_; // log_index -> Client Conn
    std::unordered_map<uint64_t, std::weak_ptr<Connection>> pending_replies_;      // req_id -> Inbound Conn
};