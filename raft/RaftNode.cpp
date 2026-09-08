#include "RaftNode.h"
#include "protocol/RaftDispatcher.h"
#include "protocol/Codec.h"
#include "protocol/wire_protocol.h"
#include "lsmtree/src/Slice.h"
#include "protocol/Metrics.h"
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
    else if (core_->GetRole() == RaftRole::kPreCandidate) role_str = "PreCandidate";

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

RaftNode::RaftNode(EventLoop* loop, uint32_t node_id, const std::vector<uint32_t>& peer_ids, 
                   raft_rpc::RaftDispatcher* dispatcher, const std::string& storage_dir)
    : loop_(loop), node_id_(node_id), dispatcher_(dispatcher),
      storage_adapter_(storage_dir.empty() ? ("node_" + std::to_string(node_id) + "_storage") : storage_dir),
      tick_timer_(loop) {

    core_ = std::make_unique<RaftCore>(node_id_, peer_ids);

    HardState hs = storage_adapter_.GetHardState();
    auto logs = storage_adapter_.LoadLogs();

    core_->Restore(hs, std::move(logs));

    Snapshot snap = storage_adapter_.GetSnapshot();
    last_snapshot_index_ = snap.meta.index;
    last_snapshot_time_sec_ = GetCurrentSec();

    storage_running_ = true;
    storage_thread_ = std::thread(&RaftNode::StorageThreadLoop, this);

    tick_timer_.SetTimerCallback([this]() {
        this->OnTick();
    });
}

RaftNode::~RaftNode() {
    storage_running_.store(false, std::memory_order_release);
    storage_queue_.Wakeup();
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
                     msg.opcode == raft_rpc::RaftOpcode::kInstallSnapshotReply ||
                     msg.opcode == raft_rpc::RaftOpcode::kPreVoteReply);

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

    for (const auto& msg : rd.messages) {
        SendOutboundMessage(msg);
    }
    rd.messages.clear();

    if (!rd.read_states.empty()) {
        ProcessReadStates(rd.read_states);
        rd.read_states.clear();
    }

    bool has_disk_work = !rd.entries.empty() || 
                         !rd.committed_entries.empty() || 
                         rd.hard_state.has_value() || 
                         rd.snapshot.is_valid;

    if (has_disk_work) {
        while (!storage_queue_.Push(std::move(rd))) {
            _mm_pause();
        }
    } else {
        {
            std::lock_guard<std::mutex> lock(core_mtx_);
            core_->Advance(0, 0);
        }
        is_persisting_.store(false, std::memory_order_release);
    }
}

void RaftNode::StorageThreadLoop() {
    while (true) {
        Ready rd;
        if (!storage_queue_.PopWait(rd, storage_running_)) {
            break;
        }

        AdvanceState state = storage_adapter_.PersistReady(rd);

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

void RaftNode::GracefulShutdown(std::function<void()> on_complete) {
    if (is_shutting_down_.exchange(true)) {
        return;
    }

    spdlog::warn("[RaftNode {}] 启动优雅停机流程...", node_id_);

    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        if (core_->IsLeader()) {
            core_->StepDown(core_->GetCurrentTerm() + 1);
        }
    }

    ScheduleCheckAndAdvance();

    std::thread drain_thread([this, on_complete = std::move(on_complete)]() {
        spdlog::info("[RaftNode {}] 开始排空持久化任务队列 (当前积压: {})...", 
                     node_id_, storage_queue_.Size());

        storage_running_.store(false, std::memory_order_release);
        storage_queue_.Wakeup();

        if (storage_thread_.joinable()) {
            storage_thread_.join();
        }

        spdlog::info("[RaftNode {}] 正在执行全引擎物理 ForceSync...", node_id_);
        storage_adapter_.ForceSync();

        spdlog::info("[RaftNode {}] 存储管线排空完成，调度主循环退出。", node_id_);

        if (loop_) {
            loop_->QueueInLoop(on_complete);
        }
    });
    drain_thread.detach();
}

void RaftNode::OnPersistFinished(Ready rd, AdvanceState state) {
    uint64_t current_applied = 0;

    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        core_->Advance(state.stabled_index, state.applied_index);
        current_applied = state.applied_index;
    }

    CheckWaitingAppliedReads(current_applied);

    if (rd.snapshot.is_valid) {
        is_snapshotting_.store(false, std::memory_order_release);
        spdlog::info("[RaftNode {}] 后台快照落盘成功完成, index: {}", node_id_, rd.snapshot.meta.index);
    }

    for (const auto& entry : rd.committed_entries) {
        if (entry.index > state.applied_index) break;

        PendingClientWrite pending_write;
        bool found = false;
        {
            std::lock_guard<std::mutex> cl(client_mtx_);
            auto it = client_wait_list_.find(entry.index);
            if (it != client_wait_list_.end()) {
                pending_write = std::move(it->second);
                client_wait_list_.erase(it);
                found = true;
            }

            if (pending_replies_.size() > 10000) {
                pending_replies_.clear();
            }
        }

        if (found) {
            auto now = std::chrono::steady_clock::now();
            uint64_t lat_us = std::chrono::duration_cast<std::chrono::microseconds>(now - pending_write.start_time).count();
            Metrics::Instance().ObserveCommitLatency(lat_us);
            Metrics::Instance().IncCommittedOps();

            if (pending_write.conn && pending_write.conn->IsConnected()) {
                auto parsed = raft_node::WireProtocol::Parse(entry.data);
                if (parsed.has_value()) {
                    auto [header, key_view, val_view, skip] = *parsed;
                    // 动态提取原 header.opcode 序列化回包，支持 WRITE_BATCH 准确应答
                    std::string reply = raft_node::WireProtocol::Serialize(
                        static_cast<raft_node::Opcode>(header.opcode), Slice(key_view), "OK_COMMIT", header.req_id
                    );
                    pending_write.conn->Send(reply);
                }
            }
        }
    }

    CheckAndTriggerSnapshot(current_applied);
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

void RaftNode::CheckAndTriggerSnapshot(uint64_t current_applied) {
    if (is_snapshotting_.load(std::memory_order_relaxed)) {
        return;
    }

    uint64_t uncompacted_count = (current_applied > last_snapshot_index_) ? (current_applied - last_snapshot_index_) : 0;
    uint64_t now_sec = GetCurrentSec();

    bool count_reach = (uncompacted_count >= kSnapshotCountThreshold);
    bool time_reach  = (uncompacted_count > 0 && (now_sec - last_snapshot_time_sec_) >= kSnapshotIntervalSec);

    if (count_reach || time_reach) {
        TriggerSnapshot(current_applied);
    }
}

void RaftNode::TriggerSnapshot(uint64_t compact_index) {
    if (compact_index <= last_snapshot_index_) {
        return;
    }

    uint64_t compact_term = storage_adapter_.GetLogTerm(compact_index);

    is_snapshotting_.store(true, std::memory_order_release);
    last_snapshot_index_ = compact_index;
    last_snapshot_time_sec_ = GetCurrentSec();

    spdlog::info("[RaftNode {}] 触发快照生成, compact_index: {}, compact_term: {}", node_id_, compact_index, compact_term);

    Ready snap_rd;
    snap_rd.snapshot.meta.index = compact_index;
    snap_rd.snapshot.meta.term = compact_term;
    snap_rd.snapshot.is_valid = true;

    while (!storage_queue_.Push(std::move(snap_rd))) {
        _mm_pause();
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

    CleanupExpiredReads();

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

void RaftNode::HandlePreVote(const std::shared_ptr<Connection>& conn, uint64_t req_id, const raft_rpc::PreVoteArgs& args) {
    spdlog::info("[RaftNode {}] 收到来自 Node {} 的 PreVote (Term: {}, req_id: {})", 
                 node_id_, args.candidate_id, args.term, req_id);

    UpdateInboundConnection(args.candidate_id, conn);
    {
        std::lock_guard<std::mutex> cl(client_mtx_);
        pending_replies_[req_id] = conn;
    }
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        core_->Step(args.candidate_id, raft_rpc::RaftOpcode::kPreVote, req_id, args.Serialize());
    }
    ScheduleCheckAndAdvance();
}

void RaftNode::HandlePreVoteReply(uint64_t req_id, const raft_rpc::PreVoteReply& reply) {
    spdlog::info("[RaftNode {}] 收到来自 Node {} 的 PreVoteReply (granted: {}, term: {})", 
                 node_id_, reply.voter_id, reply.vote_granted, reply.term);

    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        core_->Step(reply.voter_id, raft_rpc::RaftOpcode::kPreVoteReply, req_id, reply.Serialize());
    }
    ScheduleCheckAndAdvance();
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

bool RaftNode::Propose(const Slice& key, std::string_view val, const std::shared_ptr<Connection>& client_conn, uint64_t req_id) {
    if (is_shutting_down_.load(std::memory_order_relaxed)) {
        Metrics::Instance().IncRejectedOps();
        return false;
    }
    std::string cmd = raft_node::WireProtocol::Serialize(
        raft_node::Opcode::PUT_RAW,
        key,
        val,
        req_id
    );

    uint64_t assigned_index = 0;

    {
        std::lock_guard<std::mutex> lock(core_mtx_);

        if (!core_->IsLeader()) {
            Metrics::Instance().IncRejectedOps();
            return false;
        }

        uint64_t uncommitted_count = core_->GetLastLogIndex() - core_->GetCommitIndex();
        if (uncommitted_count > 10000) {
            Metrics::Instance().IncRejectedOps();
            return false;
        }

        if (!core_->Propose(cmd)) {
            Metrics::Instance().IncRejectedOps();
            return false;
        }

        assigned_index = core_->GetLastLogIndex();
    }

    {
        std::lock_guard<std::mutex> cl(client_mtx_);
        if (client_wait_list_.size() > 20000) {
            auto it = client_wait_list_.begin();
            client_wait_list_.erase(it);
        }
        client_wait_list_[assigned_index] = PendingClientWrite{client_conn, std::chrono::steady_clock::now()};
    }

    ScheduleCheckAndAdvance();
    return true;
}

bool RaftNode::ProposeBatch(std::string_view full_packet, const std::shared_ptr<Connection>& client_conn, uint64_t req_id) {
    if (is_shutting_down_.load(std::memory_order_relaxed)) {
        Metrics::Instance().IncRejectedOps();
        return false;
    }

    uint64_t assigned_index = 0;
    {
        std::lock_guard<std::mutex> lock(core_mtx_);

        if (!core_->IsLeader()) {
            Metrics::Instance().IncRejectedOps();
            return false;
        }

        uint64_t uncommitted_count = core_->GetLastLogIndex() - core_->GetCommitIndex();
        if (uncommitted_count > 10000) {
            Metrics::Instance().IncRejectedOps();
            return false;
        }

        // 整个批处理包作为一个独立的原子 Raft 日志记录
        if (!core_->Propose(std::string(full_packet))) {
            Metrics::Instance().IncRejectedOps();
            return false;
        }

        assigned_index = core_->GetLastLogIndex();
    }

    {
        std::lock_guard<std::mutex> cl(client_mtx_);
        if (client_wait_list_.size() > 20000) {
            auto it = client_wait_list_.begin();
            client_wait_list_.erase(it);
        }
        client_wait_list_[assigned_index] = PendingClientWrite{client_conn, std::chrono::steady_clock::now()};
    }

    ScheduleCheckAndAdvance();
    return true;
}

bool RaftNode::ProposeRead(const Slice& key, const std::shared_ptr<Connection>& client_conn, uint64_t req_id) {
    if (is_shutting_down_.load(std::memory_order_relaxed)) {
        Metrics::Instance().IncRejectedOps();
        return false;
    }
    std::string ctx = std::to_string(node_id_) + "_" + std::to_string(next_read_ctx_id_.fetch_add(1));

    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        if (!core_->ProposeReadIndex(ctx)) {
            Metrics::Instance().IncRejectedOps();
            return false;
        }
    }

    {
        std::lock_guard<std::mutex> cl(client_mtx_);
        pending_client_reads_[ctx] = PendingClientRead{
            ctx, key.ToString(), req_id, client_conn, std::chrono::steady_clock::now()
        };
    }

    ScheduleCheckAndAdvance();
    return true;
}

void RaftNode::CleanupExpiredReads() {
    bool is_leader = false;
    uint32_t leader_id = 0;
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        is_leader = core_->IsLeader();
        leader_id = core_->GetLeaderId();
    }

    auto now = std::chrono::steady_clock::now();
    uint16_t leader_port = (leader_id != RaftCore::kNoLeader) ? (8880 + static_cast<uint16_t>(leader_id) + 1000) : 0;
    std::string reject_payload = "REJECT:" + std::to_string(leader_port);

    struct RejectReadItem {
        std::string key;
        std::shared_ptr<Connection> conn;
        uint64_t req_id;
    };
    std::vector<RejectReadItem> reject_reads;
    std::vector<std::shared_ptr<Connection>> reject_writes;

    {
        std::lock_guard<std::mutex> cl(client_mtx_);
        if (!is_leader) {
            for (auto& [ctx, req] : pending_client_reads_) {
                if (auto conn = req.conn.lock()) {
                    reject_reads.push_back({std::move(req.key), std::move(conn), req.client_req_id});
                }
            }
            pending_client_reads_.clear();

            for (auto& req : waiting_applied_reads_) {
                if (auto conn = req.conn.lock()) {
                    reject_reads.push_back({std::move(req.key), std::move(conn), req.client_req_id});
                }
            }
            waiting_applied_reads_.clear();

            for (auto& [idx, item] : client_wait_list_) {
                if (item.conn && item.conn->IsConnected()) {
                    reject_writes.push_back(item.conn);
                }
            }
            client_wait_list_.clear();
        } else {
            for (auto it = pending_client_reads_.begin(); it != pending_client_reads_.end();) {
                if (std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second.start_time).count() > 2000) {
                    if (auto conn = it->second.conn.lock()) {
                        reject_reads.push_back({it->second.key, conn, it->second.client_req_id});
                    }
                    it = pending_client_reads_.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }

    for (const auto& item : reject_reads) {
        Metrics::Instance().IncRejectedOps();
        item.conn->Send(raft_node::WireProtocol::Serialize(
            raft_node::Opcode::GET_RAW, Slice(item.key), reject_payload, item.req_id
        ));
    }

    for (const auto& conn : reject_writes) {
        Metrics::Instance().IncRejectedOps();
        conn->Send(raft_node::WireProtocol::Serialize(
            raft_node::Opcode::PUT_RAW, Slice(""), reject_payload, 0
        ));
    }
}

void RaftNode::ProcessReadStates(const std::vector<ReadState>& read_states) {
    uint64_t current_applied = 0;
    {
        std::lock_guard<std::mutex> lock(core_mtx_);
        current_applied = core_->GetLastApplied();
    }

    std::vector<std::pair<PendingClientRead, bool>> matched_reads;
    {
        std::lock_guard<std::mutex> cl(client_mtx_);
        for (const auto& rs : read_states) {
            auto it = pending_client_reads_.find(rs.request_ctx);
            if (it == pending_client_reads_.end()) continue;

            PendingClientRead req = std::move(it->second);
            pending_client_reads_.erase(it);

            if (current_applied >= rs.read_index) {
                matched_reads.push_back({std::move(req), true});
            } else {
                waiting_applied_reads_.push_back(WaitingApplyRead{rs.read_index, req.key, req.client_req_id, req.conn});
            }
        }
    }

    for (const auto& [req, is_ready] : matched_reads) {
        auto conn = req.conn.lock();
        if (conn && conn->IsConnected()) {
            Metrics::Instance().IncReadOps();
            std::string val = storage_adapter_.StateMachineGet(Slice(req.key));
            std::string reply = raft_node::WireProtocol::Serialize(
                raft_node::Opcode::GET_RAW, Slice(req.key), val, req.client_req_id
            );
            conn->Send(reply);
        }
    }
}

void RaftNode::CheckWaitingAppliedReads(uint64_t current_applied) {
    std::vector<WaitingApplyRead> ready_list;
    {
        std::lock_guard<std::mutex> cl(client_mtx_);
        auto it = waiting_applied_reads_.begin();
        while (it != waiting_applied_reads_.end()) {
            if (current_applied >= it->read_index) {
                ready_list.push_back(std::move(*it));
                it = waiting_applied_reads_.erase(it);
            } else {
                ++it;
            }
        }
    }

    for (const auto& item : ready_list) {
        auto conn = item.conn.lock();
        if (conn && conn->IsConnected()) {
            Metrics::Instance().IncReadOps();
            std::string val = storage_adapter_.StateMachineGet(Slice(item.key));
            std::string reply = raft_node::WireProtocol::Serialize(
                raft_node::Opcode::GET_RAW, Slice(item.key), val, item.client_req_id
            );
            conn->Send(reply);
        }
    }
}