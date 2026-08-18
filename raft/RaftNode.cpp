#include "RaftNode.h"
#include "protocol/RaftDispatcher.h"
#include "protocol/Codec.h"
#include "protocol/wire_protocol.h"
#include <spdlog/spdlog.h>

inline uint64_t GetCurrentSec() {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

RaftNode::NodeStatus RaftNode::GetStatus() {
    std::lock_guard<std::mutex> lock(core_mtx_);
    std::string role_str = "Follower";
    if (core_->GetRole() == RaftRole::kLeader) role_str = "Leader";
    else if (core_->GetRole() == RaftRole::kCandidate) role_str = "Candidate";

    return NodeStatus{
        node_id_,
        role_str,
        core_->GetCurrentTerm(),
        core_->GetLeaderId(),
        core_->GetLastLogIndex(),
        core_->GetCommitIndex(),
        core_->GetStabledIndex(),
        core_->GetLastApplied()
    };
}

RaftNode::RaftNode(EventLoop* loop, uint32_t node_id, const std::vector<uint32_t>& peer_ids, raft_rpc::RaftDispatcher* dispatcher)
    : loop_(loop), node_id_(node_id), dispatcher_(dispatcher),
      storage_adapter_("node_" + std::to_string(node_id) + "_storage"),
      tick_timer_(loop) {

    core_ = std::make_unique<RaftCore>(node_id_, peer_ids);

    //  1. 从磁盘加载持久化状态与 WAL 日志
    HardState hs = storage_adapter_.GetHardState();
    auto logs = storage_adapter_.LoadLogs();

    //  2. 完全对齐 RaftCore::Restore(const HardState& hs, std::vector<raft_rpc::LogEntry> logs)
    core_->Restore(hs, std::move(logs));

    //  3. 加载快照元数据以维护 RaftNode 的快照水位
    Snapshot snap = storage_adapter_.GetSnapshot();
    last_snapshot_index_ = snap.meta.index;
    last_snapshot_time_sec_ = GetCurrentSec();

    //  4. 启动内嵌的后台落盘线程
    storage_running_ = true;
    storage_thread_ = std::thread(&RaftNode::StorageThreadLoop, this);

    tick_timer_.SetTimerCallback([this]() {
        this->OnTick();
    });
}

RaftNode::~RaftNode() {
    // 通知后台落盘线程退出并等待回收
    storage_running_ = false;
    {
        std::lock_guard<std::mutex> lock(storage_queue_mtx_);
        storage_cv_.notify_all();
    }
    if (storage_thread_.joinable()) {
        storage_thread_.join();
    }
}

void RaftNode::AddPeer(uint32_t peer_id, const std::string& ip, uint16_t port) {
    peers_.push_back(Peer{peer_id, ip, port, nullptr});
}

void RaftNode::Start() {
    for (size_t i = 0; i < peers_.size(); ++i) {
        uint32_t target_peer_id = peers_[i].id;
        peers_[i].client = std::make_shared<RpcClient>(loop_, peers_[i].ip, peers_[i].port);
        
        peers_[i].client->SetMessageCallback([this](const std::shared_ptr<Connection>& c, Buffer* buf) {
            this->dispatcher_->OnMessage(c, buf);
        });

        peers_[i].client->SetConnectionCompleteCallback([this, i, target_peer_id](const std::shared_ptr<Connection>& conn) {
            this->peers_[i].outbound_conn = conn;
        });

        peers_[i].client->Connect();
    }
    tick_timer_.Reset(10); 
}

void RaftNode::SendOutboundMessage(const OutboundMessage& msg) {
    bool is_reply = (msg.opcode == raft_rpc::RaftOpcode::kRequestVoteReply ||
                     msg.opcode == raft_rpc::RaftOpcode::kAppendEntriesReply ||
                     msg.opcode == raft_rpc::RaftOpcode::kInstallSnapshotReply);

    std::shared_ptr<Connection> reply_conn = nullptr;

    if (is_reply) {
        std::lock_guard<std::mutex> cl(client_mtx_);
        auto it = pending_replies_.find(msg.req_id);
        if (it != pending_replies_.end()) {
            reply_conn = it->second.lock();
            pending_replies_.erase(it);
        }
    }

    if (reply_conn && reply_conn->IsConnected()) {
        std::string packed = raft_rpc::Codec::Encode(static_cast<uint8_t>(msg.opcode), msg.req_id, msg.payload);
        
        EventLoop* conn_loop = reply_conn->GetLoop(); 
        if (conn_loop) {
            conn_loop->RunInLoop([reply_conn, packed = std::move(packed)]() {
                if (reply_conn->IsConnected()) {
                    reply_conn->Send(packed);
                }
            });
        }
        return;
    }

    for (auto& peer : peers_) {
        if (peer.id == msg.to_node_id) {
            if (peer.client && peer.client->IsConnected()) {
                if (msg.opcode == raft_rpc::RaftOpcode::kAppendEntries) {
                    spdlog::debug("[RaftNode {}] 发送 AppendEntries 心跳 (req_id: {}) -> Node {}", node_id_, msg.req_id, peer.id);
                }
                peer.client->Send(static_cast<uint8_t>(msg.opcode), msg.req_id, msg.payload);
            }
            break;
        }
    }
}

void RaftNode::ScheduleCheckAndAdvance() {
    bool expected = false;
    if (is_ready_scheduled_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        if (loop_) {
            loop_->QueueInLoop([this]() {
                this->DoCheckAndAdvance();
            });
        }
    }
}

void RaftNode::DoCheckAndAdvance() {
    CheckAndAdvance();
    is_ready_scheduled_.store(false, std::memory_order_release);
}


void RaftNode::CheckAndAdvance() {
    // 1. 尝试抢占落盘屏障：如果后台 StorageThread 正在 Persist，暂不 PopReady
    // 保护 storage_queue_ 深度严格 <= 1
    bool expected = false;
    if (!is_persisting_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return; 
    }

    Ready rd;
    bool has_ready = false;
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        if (core_->HasReady()) {
            rd = core_->PopReady();
            has_ready = true;
        }
    }

    if (!has_ready) {
        is_persisting_.store(false, std::memory_order_release);
        return;
    }

    // 2. 网络 RPC 永远第一时间无条件发送（心跳直通网卡，绝对零延迟！）
    for (const auto& msg : rd.messages) {
        SendOutboundMessage(msg);
    }
    rd.messages.clear();

    // 3. 检查是否有真正的物理磁盘写任务
    bool has_disk_work = !rd.entries.empty() || 
                         !rd.committed_entries.empty() || 
                         rd.hard_state.has_value() || 
                         rd.snapshot.is_valid;

    if (has_disk_work) {
        std::lock_guard<std::mutex> lock(storage_queue_mtx_);
        storage_queue_.push(std::move(rd));
        storage_cv_.notify_one();
        // is_persisting_ 保持为 true，由 OnPersistFinished 执行完毕后释放
    } else {
        // 纯内存任务，立刻 Advance 并释放屏障
        {
            std::lock_guard<std::mutex> lock(core_mtx_);
            core_->Advance(0, 0);
        }
        is_persisting_.store(false, std::memory_order_release);
    }
}

// 内嵌后台落盘线程：专职处理磁盘 WAL / DB 写入 / Snapshot 落盘
void RaftNode::StorageThreadLoop() {
    while (storage_running_) {
        Ready rd;
        {
            std::unique_lock<std::mutex> lock(storage_queue_mtx_);
            storage_cv_.wait(lock, [this] {
                return !storage_queue_.empty() || !storage_running_;
            });
            if (!storage_running_ && storage_queue_.empty()) break;

            rd = std::move(storage_queue_.front());
            storage_queue_.pop();
        }

        // 物理落盘 (若 rd 含有 snapshot，适配器内部自动做 SST 导出与 snap 元数据存储)
        AdvanceState state = storage_adapter_.PersistReady(rd);

        // 切回 EventLoop 线程回调
        if (loop_) {
            std::shared_ptr<RaftNode> self = nullptr;
            try {
                self = shared_from_this();
            } catch (const std::bad_weak_ptr&) {}

            auto rd_ptr = std::make_shared<Ready>(std::move(rd));
            loop_->QueueInLoop([this, self, rd_ptr, state]() {
                this->OnPersistFinished(std::move(*rd_ptr), state);
            });
        }
    }
}

void RaftNode::OnPersistFinished(Ready rd, AdvanceState state) {
    uint64_t current_applied = 0;

    // 1. 推进 Raft 核心水位，RaftCore 在 Advance 内部会自动释放已落盘的 unstable 内存
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        core_->Advance(state.stabled_index, state.applied_index);
        current_applied = state.applied_index;
    }

    // 若当前 Ready 包含 Snapshot，说明 StorageAdapter 已完成物理快照落盘，解除快照屏障
    if (rd.snapshot.is_valid) {
        is_snapshotting_.store(false, std::memory_order_release);
        spdlog::info("[RaftNode {}] 后台快照落盘成功完成, index: {}", node_id_, rd.snapshot.meta.index);
    }

    // 2. 给 Client 发送 ACK 并清理等待映射表
    for (const auto& entry : rd.committed_entries) {
        if (entry.index > state.applied_index) break;

        std::shared_ptr<Connection> client_conn;
        {
            std::lock_guard<std::mutex> cl(client_mtx_);
            auto it = client_wait_list_.find(entry.index);
            if (it != client_wait_list_.end()) {
                client_conn = it->second;
                client_wait_list_.erase(it);
            }

            // 定期兜底清理异常丢弃的 pending_replies_，防止隐式内存泄露
            if (pending_replies_.size() > 10000) {
                pending_replies_.clear();
            }
        }

        if (client_conn && client_conn->IsConnected()) {
            auto parsed = raft_node::WireProtocol::Parse(entry.data);
            int32_t raw_key = parsed ? std::get<0>(*parsed).key : 0;
            std::string reply = raft_node::WireProtocol::Serialize(raft_node::Opcode::PUT_RAW, raw_key, "OK_COMMIT");
            client_conn->Send(reply);
        }
    }

    // 3. 多维快照门限检测
    CheckAndTriggerSnapshot(current_applied);

    // 4. 正确重置 Persist 屏障并检查追帧
    is_persisting_.store(false, std::memory_order_release);

    bool has_ready = false;
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        has_ready = core_->HasReady();
    }
    if (has_ready) {
        ScheduleCheckAndAdvance();
    }
}

//  检查是否达到快照门限
void RaftNode::CheckAndTriggerSnapshot(uint64_t current_applied) {
    if (is_snapshotting_.load(std::memory_order_relaxed)) {
        return; // 正在生成快照中，跳过
    }

    uint64_t uncompacted_count = (current_applied > last_snapshot_index_) ? (current_applied - last_snapshot_index_) : 0;
    uint64_t now_sec = GetCurrentSec();

    bool count_reach = (uncompacted_count >= kSnapshotCountThreshold);
    bool time_reach  = (uncompacted_count > 0 && (now_sec - last_snapshot_time_sec_) >= kSnapshotIntervalSec);

    if (count_reach || time_reach) {
        TriggerSnapshot(current_applied);
    }
}

//  触发快照：从 StorageAdapter 获取 Term，完全不依赖非公有 RaftCore 接口
void RaftNode::TriggerSnapshot(uint64_t compact_index) {
    if (compact_index <= last_snapshot_index_) {
        return;
    }

    // 从存储适配器安全获取该 Index 的 Term
    uint64_t compact_term = storage_adapter_.GetLogTerm(compact_index);

    is_snapshotting_.store(true, std::memory_order_release);
    last_snapshot_index_ = compact_index;
    last_snapshot_time_sec_ = GetCurrentSec();

    spdlog::info("[RaftNode {}] 触发快照生成, compact_index: {}, compact_term: {}", node_id_, compact_index, compact_term);

    // 构造 Snapshot Ready，塞入后台队列异步落盘（零卡顿 EventLoop）
    Ready snap_rd;
    snap_rd.snapshot.meta.index = compact_index;
    snap_rd.snapshot.meta.term = compact_term;
    snap_rd.snapshot.is_valid = true;

    {
        std::lock_guard<std::mutex> lock(storage_queue_mtx_);
        storage_queue_.push(std::move(snap_rd));
        storage_cv_.notify_one();
    }
}

void RaftNode::OnTick() {
    for (auto& peer : peers_) {
        if (!peer.GetActiveConnection() && peer.client && !peer.client->IsConnected()) {
            peer.client->Connect();
        }
    }
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        core_->Tick();
    }

    ScheduleCheckAndAdvance();
    tick_timer_.Reset(10);
}

void RaftNode::UpdateInboundConnection(uint32_t peer_id, const std::shared_ptr<Connection>& conn) {
    for (auto& peer : peers_) {
        if (peer.id == peer_id) {
            peer.inbound_conn = conn;
            break;
        }
    }
}

void RaftNode::HandleAppendEntries(const std::shared_ptr<Connection>& conn, uint64_t req_id, const raft_rpc::AppendEntriesArgs& args) {
    spdlog::debug("[RaftNode {}] 收到来自 Leader (Node {}) 的 AppendEntries 心跳 (Term: {}, req_id: {})", 
                  node_id_, args.leader_id, args.term, req_id);

    UpdateInboundConnection(args.leader_id, conn);
    {
        std::lock_guard<std::mutex> cl(client_mtx_);
        pending_replies_[req_id] = conn;
    }
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        core_->Step(args.leader_id, raft_rpc::RaftOpcode::kAppendEntries, req_id, args.Serialize());
    }
    ScheduleCheckAndAdvance();
}

void RaftNode::HandleAppendEntriesReply(uint64_t req_id, const raft_rpc::AppendEntriesReply& reply) {
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        core_->Step(reply.follower_id, raft_rpc::RaftOpcode::kAppendEntriesReply, req_id, reply.Serialize());
    }
    ScheduleCheckAndAdvance();
}

void RaftNode::HandleRequestVote(const std::shared_ptr<Connection>& conn, uint64_t req_id, const raft_rpc::RequestVoteArgs& args) {
    spdlog::debug("[RaftNode {}] 收到来自 Node {} 的 RequestVote (Term: {})", node_id_, args.candidate_id, args.term);

    UpdateInboundConnection(args.candidate_id, conn);
    {
        std::lock_guard<std::mutex> cl(client_mtx_);
        pending_replies_[req_id] = conn;
    }
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        core_->Step(args.candidate_id, raft_rpc::RaftOpcode::kRequestVote, req_id, args.Serialize());
    }
    ScheduleCheckAndAdvance();
}

void RaftNode::HandleRequestVoteReply(uint64_t req_id, const raft_rpc::RequestVoteReply& reply) {
    spdlog::debug("[RaftNode {}] 收到来自 Node {} 的 RequestVoteReply (granted: {}, term: {})", 
                  node_id_, reply.voter_id, reply.vote_granted, reply.term);

    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        core_->Step(reply.voter_id, raft_rpc::RaftOpcode::kRequestVoteReply, req_id, reply.Serialize());
    }
    ScheduleCheckAndAdvance();
}

void RaftNode::HandleInstallSnapshot(const std::shared_ptr<Connection>& conn, uint64_t req_id, const raft_rpc::InstallSnapshotArgs& args) {
    UpdateInboundConnection(args.leader_id, conn);
    {
        std::lock_guard<std::mutex> cl(client_mtx_);
        pending_replies_[req_id] = conn;
    }
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        core_->Step(args.leader_id, raft_rpc::RaftOpcode::kInstallSnapshot, req_id, args.Serialize());
    }
    ScheduleCheckAndAdvance();
}

void RaftNode::HandleInstallSnapshotReply(uint64_t req_id, const raft_rpc::InstallSnapshotReply& reply) {
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        core_->Step(reply.follower_id, raft_rpc::RaftOpcode::kInstallSnapshotReply, req_id, reply.Serialize());
    }
    ScheduleCheckAndAdvance();
}

bool RaftNode::Propose(int32_t key, std::string_view val, const std::shared_ptr<Connection>& client_conn) {
    std::string cmd = raft_node::WireProtocol::Serialize(
        raft_node::Opcode::PUT_RAW,
        key,
        val
    );

    uint64_t assigned_index = 0;

    {
        std::lock_guard<std::mutex> lock(core_mtx_);

        if (!core_->IsLeader()) {
            return false;
        }

        // 🚀 高水位硬拦截：未提交日志超过 10000 条（约 40MB）直接拒绝，防止 OOM
        uint64_t uncommitted_count = core_->GetLastLogIndex() - core_->GetCommitIndex();
        if (uncommitted_count > 10000) {
            return false;
        }

        if (!core_->Propose(cmd)) {
            return false;
        }

        assigned_index = core_->GetLastLogIndex();
    }

    {
        std::lock_guard<std::mutex> cl(client_mtx_);
        // 防御性清理悬挂连接
        if (client_wait_list_.size() > 20000) {
            auto it = client_wait_list_.begin();
            client_wait_list_.erase(it);
        }
        client_wait_list_[assigned_index] = client_conn;
    }

    ScheduleCheckAndAdvance();
    return true;
}


bool RaftNode::ProposeRead(int32_t key, const std::shared_ptr<Connection>& client_conn) {
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        if (!core_->IsLeader()) {
            return false;
        }
    }

    std::string val = storage_adapter_.StateMachineGet(key);
    if (client_conn && client_conn->IsConnected()) {
        std::string reply = raft_node::WireProtocol::Serialize(
            raft_node::Opcode::GET_RAW,
            key,
            val
        );
        client_conn->Send(reply);
    }
    return true;
}