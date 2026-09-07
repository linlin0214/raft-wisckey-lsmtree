#include "RaftCore.h"
#include <spdlog/spdlog.h>
#include <algorithm>

RaftCore::RaftCore(uint32_t node_id, const std::vector<uint32_t>& peer_ids, uint64_t random_seed)
    : node_id_(node_id), role_(RaftRole::kFollower), leader_id_(kNoLeader),
      rng_(random_seed == 0 ? std::mt19937(std::random_device{}()) : std::mt19937(random_seed)) {
    
    peer_ids_.clear();
    for (uint32_t id : peer_ids) {
        if (id != node_id_) {
            peer_ids_.push_back(id);
        }
    }

    all_nodes_ = peer_ids_;
    all_nodes_.push_back(node_id_);
    std::sort(all_nodes_.begin(), all_nodes_.end());
    all_nodes_.erase(std::unique(all_nodes_.begin(), all_nodes_.end()), all_nodes_.end());

    spdlog::info("[RaftCore {}] 初始化完成 | 节点总数 N={}, 对端 Peer 数量={}", 
                 node_id_, all_nodes_.size(), peer_ids_.size());

    state_.term = 0;
    state_.voted_for = kNoVote;
    state_.commit_index = 0;
    state_.last_applied = 0;
    prev_hard_state_ = state_;
    
    BecomeFollower(0, kNoVote);
}

void RaftCore::Restore(const HardState& hs, std::vector<raft_rpc::LogEntry> logs) {
    state_ = hs;
    prev_hard_state_ = hs;
    hard_state_dirty_ = false;

    raft_log_.Restore(
        0,               
        0,               
        std::move(logs), 
        hs.commit_index, 
        hs.last_applied  
    );

    role_ = RaftRole::kFollower;
    leader_id_ = kNoLeader;
    ResetElectionTimer();
}

void RaftCore::ResetElectionTimer() {
    election_elapsed_ = 0;
    std::uniform_int_distribution<int> dist(50, 100);
    randomized_election_timeout_ = dist(rng_);
}

void RaftCore::Tick() {
    if (role_ == RaftRole::kLeader) {
        TickHeartbeat();
    } else {
        TickElection();
    }
}

void RaftCore::TickElection() {
    election_elapsed_++;

    if (election_elapsed_ >= randomized_election_timeout_) {
        ResetElectionTimer();
        spdlog::info("[RaftCore {}] 选举计时器超时，启动 Pre-Vote 预投票试探...", node_id_);
        BecomePreCandidate();
    }
}

void RaftCore::TickHeartbeat() {
    heartbeat_elapsed_++;
    int heartbeat_timeout = 10;
    if (heartbeat_elapsed_ >= heartbeat_timeout) {
        heartbeat_elapsed_ = 0;
        BcastAppendEntries();
    }
}

void RaftCore::BecomeFollower(uint64_t term, int32_t voted_for) {
    if (state_.term != term || state_.voted_for != voted_for) {
        hard_state_dirty_ = true;
    }
    role_ = RaftRole::kFollower;
    state_.term = term;
    state_.voted_for = voted_for;
    leader_id_ = kNoLeader;

    spdlog::info("[RaftCore {}] BecomeFollower term={} voted_for={}", node_id_, state_.term, state_.voted_for);

    ResetElectionTimer();
    voted_peers_.clear();
    pre_voted_peers_.clear();
    pending_read_indexes_.clear();
    ready_read_states_.clear();
}

void RaftCore::BecomePreCandidate() {
    role_ = RaftRole::kPreCandidate;
    leader_id_ = kNoLeader;

    ResetElectionTimer();
    pre_voted_peers_.clear();
    pre_voted_peers_.insert(node_id_);

    if (all_nodes_.size() == 1) {
        BecomeCandidate();
        return;
    }

    raft_rpc::PreVoteArgs args;
    args.term = state_.term + 1; // 试探下一个任期
    args.candidate_id = node_id_;
    args.last_log_index = raft_log_.GetLastIndex();
    args.last_log_term = raft_log_.GetLastTerm();

    std::string payload = args.Serialize();
    for (uint32_t peer_id : peer_ids_) {
        EnqueueMessage(peer_id, raft_rpc::RaftOpcode::kPreVote, GenerateReqId(), payload);
    }
}

void RaftCore::BecomeCandidate() {
    role_ = RaftRole::kCandidate;
    state_.term++;
    state_.voted_for = static_cast<int32_t>(node_id_);
    hard_state_dirty_ = true;
    leader_id_ = kNoLeader;

    spdlog::info("[RaftCore {}] BecomeCandidate term={} 已投票给自己", node_id_, state_.term);
    
    ResetElectionTimer();
    voted_peers_.clear();
    voted_peers_.insert(node_id_);
    
    if (all_nodes_.size() == 1) {
        BecomeLeader();
        return;
    }

    raft_rpc::RequestVoteArgs args;
    args.term = state_.term;
    args.candidate_id = node_id_;
    args.last_log_index = raft_log_.GetLastIndex();
    args.last_log_term = raft_log_.GetLastTerm();

    std::string payload = args.Serialize();
    for (uint32_t peer_id : peer_ids_) {
        EnqueueMessage(peer_id, raft_rpc::RaftOpcode::kRequestVote, GenerateReqId(), payload);
    }
}

void RaftCore::BecomeLeader() {
    role_ = RaftRole::kLeader;
    leader_id_ = node_id_;

    spdlog::warn("=========================================================================");
    spdlog::warn("🎉 [RaftCore {}] 成功集齐多数票，加冕登基为集群 Leader！Current Term: {}", node_id_, state_.term);
    spdlog::warn("=========================================================================");

    heartbeat_elapsed_ = 0;
    next_index_.clear();
    match_index_.clear();

    uint64_t old_last_idx = raft_log_.GetLastIndex();

    raft_rpc::LogEntry noop_entry{
        old_last_idx + 1,
        state_.term,
        raft_rpc::EntryType::kNormal,
        ""
    };

    if (!raft_log_.Append(noop_entry)) {
        spdlog::critical("[RaftCore] Leader 追加 No-op 日志失败！");
        return;
    }

    uint64_t next_idx_for_peers = old_last_idx + 1;

    for (uint32_t peer_id : peer_ids_) {
        next_index_[peer_id] = next_idx_for_peers;
        match_index_[peer_id] = 0;
    }

    match_index_[node_id_] = raft_log_.GetLastIndex();
    next_index_[node_id_] = raft_log_.GetLastIndex() + 1;

    CheckLeaderCommit();
    BcastAppendEntries();
}

void RaftCore::Step(uint32_t from_node, raft_rpc::RaftOpcode opcode, uint64_t req_id, const std::string& payload) {
    switch (opcode) {
        case raft_rpc::RaftOpcode::kPreVote:
            HandlePreVote(from_node, req_id, payload);
            break;
        case raft_rpc::RaftOpcode::kPreVoteReply:
            HandlePreVoteReply(from_node, req_id, payload);
            break;
        case raft_rpc::RaftOpcode::kAppendEntries:
            HandleAppendEntries(from_node, req_id, payload);
            break;
        case raft_rpc::RaftOpcode::kAppendEntriesReply:
            HandleAppendEntriesReply(from_node, req_id, payload);
            break;
        case raft_rpc::RaftOpcode::kRequestVote:
            HandleRequestVote(from_node, req_id, payload);
            break;
        case raft_rpc::RaftOpcode::kRequestVoteReply:
            HandleRequestVoteReply(from_node, req_id, payload);
            break;
        case raft_rpc::RaftOpcode::kInstallSnapshot:
            HandleInstallSnapshot(from_node, req_id, payload);
            break;
        case raft_rpc::RaftOpcode::kInstallSnapshotReply:
            HandleInstallSnapshotReply(from_node, req_id, payload);
            break;
        default:
            break;
    }
}

bool RaftCore::Propose(const std::string& command) {
    if (role_ != RaftRole::kLeader) {
        return false;
    }

    uint64_t new_index = raft_log_.GetLastIndex() + 1;
    raft_rpc::LogEntry entry{new_index, state_.term, raft_rpc::EntryType::kNormal, command};
    
    if (!raft_log_.Append(entry)) {
        spdlog::error("[RaftCore] Propose 日志追加失败 (index={})", new_index);
        return false;
    }

    match_index_[node_id_] = new_index;
    CheckLeaderCommit();
    return true;
}

void RaftCore::HandlePreVote(uint32_t from, uint64_t req_id, const std::string& payload) {
    auto args = raft_rpc::PreVoteArgs::Deserialize(payload);

    raft_rpc::PreVoteReply reply;
    reply.voter_id = node_id_;
    reply.vote_granted = false;

    if (args.term <= state_.term) {
        reply.term = state_.term;
        EnqueueMessage(from, raft_rpc::RaftOpcode::kPreVoteReply, req_id, reply.Serialize());
        return;
    }

    uint64_t my_last_index = raft_log_.GetLastIndex();
    uint64_t my_last_term = raft_log_.GetLastTerm();
    
    bool log_ok = (args.last_log_term > my_last_term) || 
                  (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);

    if (log_ok) {
        reply.vote_granted = true;
    }

    reply.term = state_.term;
    EnqueueMessage(from, raft_rpc::RaftOpcode::kPreVoteReply, req_id, reply.Serialize());
}

void RaftCore::HandlePreVoteReply(uint32_t from, uint64_t req_id, const std::string& payload) {
    if (role_ != RaftRole::kPreCandidate) return;

    auto reply = raft_rpc::PreVoteReply::Deserialize(payload);

    if (reply.term > state_.term) {
        BecomeFollower(reply.term, kNoVote);
        return;
    }

    if (reply.vote_granted) {
        pre_voted_peers_.insert(from);
        spdlog::debug("[RaftCore {}] 收到来自 Node {} 的 PreVote 赞同票 (当前: {}/{})", 
                      node_id_, from, pre_voted_peers_.size(), all_nodes_.size() / 2 + 1);

        if (pre_voted_peers_.size() >= all_nodes_.size() / 2 + 1) {
            spdlog::info("[RaftCore {}] 成功集齐 PreVote 多数派准入，正式发起决选！", node_id_);
            BecomeCandidate();
        }
    }
}

void RaftCore::HandleRequestVote(uint32_t from, uint64_t req_id, const std::string& payload) {
    auto args = raft_rpc::RequestVoteArgs::Deserialize(payload);

    raft_rpc::RequestVoteReply reply;
    reply.voter_id = node_id_;
    reply.vote_granted = false;

    if (args.term < state_.term) {
        reply.term = state_.term;
        EnqueueMessage(from, raft_rpc::RaftOpcode::kRequestVoteReply, req_id, reply.Serialize());
        return;
    }

    if (args.term > state_.term) {
        BecomeFollower(args.term, kNoVote);
    }

    uint64_t my_last_index = raft_log_.GetLastIndex();
    uint64_t my_last_term = raft_log_.GetLastTerm();
    
    bool log_ok = (args.last_log_term > my_last_term) || 
                  (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);

    if ((state_.voted_for == kNoVote || state_.voted_for == static_cast<int32_t>(args.candidate_id)) && log_ok) {
        state_.voted_for = static_cast<int32_t>(args.candidate_id);
        hard_state_dirty_ = true;
        reply.vote_granted = true;
        ResetElectionTimer();
    }

    // ItemHeap 防御修复：确保回包携带刷新后的最新 Term
    reply.term = state_.term;
    EnqueueMessage(from, raft_rpc::RaftOpcode::kRequestVoteReply, req_id, reply.Serialize());
}

void RaftCore::HandleRequestVoteReply(uint32_t from, uint64_t req_id, const std::string& payload) {
    if (role_ != RaftRole::kCandidate) return;

    auto reply = raft_rpc::RequestVoteReply::Deserialize(payload);

    if (reply.term < state_.term) return;

    if (reply.term > state_.term) {
        BecomeFollower(reply.term, kNoVote);
        return;
    }

    if (reply.vote_granted) {
        voted_peers_.insert(from);
        spdlog::debug("[RaftCore {}] 收到来自 Node {} 的赞成票 (当前: {}/{})", 
                      node_id_, from, voted_peers_.size(), all_nodes_.size() / 2 + 1);

        if (voted_peers_.size() >= all_nodes_.size() / 2 + 1) {
            BecomeLeader();
        }
    }
}

void RaftCore::HandleAppendEntries(uint32_t from, uint64_t req_id, const std::string& payload) {
    auto args = raft_rpc::AppendEntriesArgs::Deserialize(payload);

    raft_rpc::AppendEntriesReply reply;
    reply.follower_id = node_id_;
    reply.success = 0;

    if (args.term < state_.term) {
        reply.term = state_.term;
        EnqueueMessage(from, raft_rpc::RaftOpcode::kAppendEntriesReply, req_id, reply.Serialize());
        return;
    }

    if (args.term > state_.term) {
        BecomeFollower(args.term, kNoVote);
    } else if (role_ != RaftRole::kFollower) {
        BecomeFollower(args.term, state_.voted_for);
    }

    // ItemHeap 防御修复 1：在可能触发 BecomeFollower 之后赋值，彻底消除 reply.term 脏读旧 Term 隐患
    reply.term = state_.term;

    leader_id_ = static_cast<uint32_t>(args.leader_id);
    election_elapsed_ = 0;

    if (raft_log_.GetLastIndex() < args.prev_log_index) {
        reply.conflict_index = raft_log_.GetLastIndex() + 1;
        reply.conflict_term = 0;
        EnqueueMessage(from, raft_rpc::RaftOpcode::kAppendEntriesReply, req_id, reply.Serialize());
        return;
    }

    if (raft_log_.GetTerm(args.prev_log_index) != args.prev_log_term) {
        reply.conflict_term = raft_log_.GetTerm(args.prev_log_index);
        uint64_t conflict_idx = args.prev_log_index;
        while (conflict_idx > raft_log_.last_included_index() && 
               raft_log_.GetTerm(conflict_idx - 1) == reply.conflict_term) {
            conflict_idx--;
        }
        reply.conflict_index = conflict_idx;
        EnqueueMessage(from, raft_rpc::RaftOpcode::kAppendEntriesReply, req_id, reply.Serialize());
        return;
    }

    std::vector<raft_rpc::LogEntry> new_entries = raft_rpc::DeserializeEntries(args.entries);
    raft_log_.Append(std::move(new_entries));

    if (args.leader_commit > raft_log_.commit_index()) {
        raft_log_.set_commit_index(std::min(args.leader_commit, raft_log_.GetLastIndex()));
    }

    reply.success = 1;
    reply.match_index = raft_log_.GetLastIndex();
    EnqueueMessage(from, raft_rpc::RaftOpcode::kAppendEntriesReply, req_id, reply.Serialize());
}

void RaftCore::HandleAppendEntriesReply(uint32_t from, uint64_t req_id, const std::string& payload) {
    if (role_ != RaftRole::kLeader) return;

    auto reply = raft_rpc::AppendEntriesReply::Deserialize(payload);

    if (reply.term < state_.term) return;

    if (reply.term > state_.term) {
        BecomeFollower(reply.term, kNoVote);
        return;
    }

    //  收集当前任期心跳 ACK，推进 ReadIndex 确认
    if (!pending_read_indexes_.empty()) {
        auto it = pending_read_indexes_.begin();
        while (it != pending_read_indexes_.end()) {
            it->acks.insert(from);
            // 达成法定过半数 Quorum 确认
            if (it->acks.size() >= (all_nodes_.size() / 2 + 1)) {
                ready_read_states_.push_back(ReadState{it->read_index, it->ctx});
                it = pending_read_indexes_.erase(it);
            } else {
                ++it;
            }
        }
    }

    if (reply.success == 1) {
        if (reply.match_index > match_index_[from]) {
            match_index_[from] = reply.match_index;
            next_index_[from] = reply.match_index + 1;
            CheckLeaderCommit();

            if (next_index_[from] <= raft_log_.GetLastIndex()) {
                SendAppendEntriesTo(from, GenerateReqId());
            }
        }
    } else {
        uint64_t fallback_idx = (reply.conflict_index > 0) ? reply.conflict_index : (next_index_[from] - 1);
        
        // ItemHeap 防御修复 2：加入下界保护 (raft_log_.last_included_index() + 1)，防止下溢翻转为 UINT64_MAX
        uint64_t min_allowed_next = raft_log_.last_included_index() + 1;
        uint64_t safe_target = std::max(match_index_[from] + 1, fallback_idx);
        next_index_[from] = std::max(min_allowed_next, std::min(next_index_[from] - 1, safe_target));

        SendAppendEntriesTo(from, GenerateReqId());
    }
}

void RaftCore::HandleInstallSnapshot(uint32_t from, uint64_t req_id, const std::string& payload) {
    auto args = raft_rpc::InstallSnapshotArgs::Deserialize(payload);

    raft_rpc::InstallSnapshotReply reply;
    reply.follower_id = node_id_;
    reply.success = 0;

    if (args.term < state_.term) {
        reply.term = state_.term;
        EnqueueMessage(from, raft_rpc::RaftOpcode::kInstallSnapshotReply, req_id, reply.Serialize());
        return;
    }

    if (args.term > state_.term) {
        BecomeFollower(args.term, kNoVote);
    } else if (role_ != RaftRole::kFollower) {
        BecomeFollower(args.term, state_.voted_for);
    }

    // ItemHeap 防御修复：刷新 reply.term
    reply.term = state_.term;
    leader_id_ = static_cast<uint32_t>(args.leader_id);
    election_elapsed_ = 0;

    if (args.last_included_index <= raft_log_.commit_index()) {
        reply.success = 1; // ItemHeap 已被覆盖或包含视为成功
        EnqueueMessage(from, raft_rpc::RaftOpcode::kInstallSnapshotReply, req_id, reply.Serialize());
        return;
    }

    raft_log_.TruncatePrefix(args.last_included_index, args.last_included_term);

    // ItemHeap 防御修复 3：快照安装成功显式标记 success = 1，避免 Leader 视角判定失败
    reply.success = 1;
    EnqueueMessage(from, raft_rpc::RaftOpcode::kInstallSnapshotReply, req_id, reply.Serialize());
}

void RaftCore::HandleInstallSnapshotReply(uint32_t from, uint64_t req_id, const std::string& payload) {
    if (role_ != RaftRole::kLeader) return;

    auto reply = raft_rpc::InstallSnapshotReply::Deserialize(payload);

    if (reply.term < state_.term) return;

    if (reply.term > state_.term) {
        BecomeFollower(reply.term, kNoVote);
        return;
    }

    if (reply.success == 1) {
        uint64_t snapshot_index = raft_log_.last_included_index();
        if (snapshot_index > match_index_[from]) {
            match_index_[from] = snapshot_index;
            next_index_[from] = snapshot_index + 1;
            CheckLeaderCommit();
        }
    }
}

void RaftCore::CheckLeaderCommit() {
    if (all_nodes_.size() == 3) {
        uint64_t m0 = match_index_[all_nodes_[0]];
        uint64_t m1 = match_index_[all_nodes_[1]];
        uint64_t m2 = match_index_[all_nodes_[2]];

        uint64_t quorum_index = (m0 < m1) ? ((m1 < m2) ? m1 : std::max(m0, m2))
                                          : ((m0 < m2) ? m0 : std::max(m1, m2));

        if (quorum_index > raft_log_.commit_index() && raft_log_.GetTerm(quorum_index) == state_.term) {
            raft_log_.set_commit_index(quorum_index);
        }
        return;
    }

    std::vector<uint64_t> matches;
    matches.reserve(all_nodes_.size());
    for (uint32_t id : all_nodes_) {
        matches.push_back(match_index_[id]);
    }
    
    size_t median_pos = matches.size() / 2;
    std::nth_element(matches.begin(), matches.begin() + median_pos, matches.end());
    
    uint64_t quorum_index = matches[median_pos];

    if (quorum_index > raft_log_.commit_index() && raft_log_.GetTerm(quorum_index) == state_.term) {
        raft_log_.set_commit_index(quorum_index);
    }
}

void RaftCore::BcastAppendEntries() {
    for (uint32_t peer_id : peer_ids_) {
        SendAppendEntriesTo(peer_id, GenerateReqId());
    }
}

void RaftCore::SendAppendEntriesTo(uint32_t peer_id, uint64_t req_id) {
    uint64_t actual_req_id = (req_id == 0) ? GenerateReqId() : req_id;

    // ItemHeap 防御修复：防止 next_index_[peer_id] 为 0 导致的无符号整数溢出
    if (next_index_[peer_id] == 0) {
        next_index_[peer_id] = raft_log_.last_included_index() + 1;
    }

    uint64_t prev_log_index = next_index_[peer_id] - 1;

    if (prev_log_index < raft_log_.last_included_index()) {
        SendInstallSnapshotTo(peer_id, actual_req_id);
        return;
    }

    uint64_t prev_log_term = raft_log_.GetTerm(prev_log_index);

    raft_rpc::AppendEntriesArgs args;
    args.term = state_.term;
    args.leader_id = node_id_;
    args.prev_log_index = prev_log_index;
    args.prev_log_term = prev_log_term;
    args.leader_commit = raft_log_.commit_index();

    auto entries_to_send = raft_log_.GetEntries(next_index_[peer_id]);
    args.entries = entries_to_send.empty() ? "" : raft_rpc::SerializeEntries(entries_to_send);

    EnqueueMessage(peer_id, raft_rpc::RaftOpcode::kAppendEntries, actual_req_id, args.Serialize());
}

void RaftCore::SendInstallSnapshotTo(uint32_t peer_id, uint64_t req_id) {
    uint64_t actual_req_id = (req_id == 0) ? GenerateReqId() : req_id;

    raft_rpc::InstallSnapshotArgs args;
    args.term = state_.term;
    args.leader_id = node_id_;
    args.last_included_index = raft_log_.last_included_index();
    args.last_included_term = raft_log_.last_included_term();

    EnqueueMessage(peer_id, raft_rpc::RaftOpcode::kInstallSnapshot, actual_req_id, args.Serialize());
}

void RaftCore::EnqueueMessage(uint32_t to, raft_rpc::RaftOpcode opcode, uint64_t req_id, const std::string& payload) {
    pending_messages_.push_back(OutboundMessage{to, opcode, req_id, payload});
}

bool RaftCore::HasReady() const {
    return hard_state_dirty_ ||
           !raft_log_.UnstableEntries().empty() ||
           !raft_log_.CommittedUnappliedEntries().empty() ||
           !pending_messages_.empty()||
           !ready_read_states_.empty();
}

Ready RaftCore::PopReady() {
    if (role_ == RaftRole::kLeader && !raft_log_.UnstableEntries().empty()) {
        BcastAppendEntries();
    }

    Ready rd;
    if (hard_state_dirty_ || state_ != prev_hard_state_) {
        rd.hard_state = state_;
    }

    rd.entries = raft_log_.UnstableEntries();
    rd.committed_entries = raft_log_.CommittedUnappliedEntries();
    rd.messages.swap(pending_messages_);
    rd.read_states.swap(ready_read_states_);

    return rd;
}

void RaftCore::Advance(uint64_t stabled_index, uint64_t applied_index) {
    if (stabled_index > 0) {
        raft_log_.StableTo(stabled_index);
    }
    if (applied_index > 0) {
        raft_log_.AppliedTo(applied_index);
    }
    prev_hard_state_ = state_;
    hard_state_dirty_ = false;
}

bool RaftCore::ProposeReadIndex(const std::string& ctx) {
    if (role_ != RaftRole::kLeader) {
        return false;
    }

    // Raft 规范：Leader 必须在当前任期至少提交过一条日志，以确认真实的 commit_index
    if (raft_log_.GetTerm(raft_log_.commit_index()) != state_.term) {
        return false;
    }

    uint64_t current_commit = raft_log_.commit_index();

    // 单节点集群无需跨网络对账，直接达成多数派确认
    if (all_nodes_.size() == 1) {
        ready_read_states_.push_back(ReadState{current_commit, ctx});
        return true;
    }

    ReadIndexRequest req;
    req.ctx = ctx;
    req.read_index = current_commit;
    req.acks.insert(node_id_); // Leader 自带 1 票

    pending_read_indexes_.push_back(std::move(req));

    // 仅在当前无等待队列时触发广播，避免高并发读场景下的网络风暴
    if (pending_read_indexes_.size() == 1) {
        BcastAppendEntries();
    }
    return true;
}

void RaftCore::StepDown(uint64_t new_term) {
    if (new_term < state_.term) {
        new_term = state_.term;
    }
    spdlog::warn("[RaftCore {}] 执行 StepDown 主动退位，Term: {} -> {}", node_id_, state_.term, new_term);
    BecomeFollower(new_term, kNoVote);
}