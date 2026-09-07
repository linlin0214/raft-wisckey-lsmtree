#include "network/EventLoop.h"
#include "network/TcpServer.h"
#include "protocol/RaftDispatcher.h"
#include "raft/RaftNode.h"
#include "protocol/wire_protocol.h"  
#include "protocol/RaftRpc.h"
#include "network/Channel.h"
#include "lsmtree/third_party/httplib.h"
#include "lsmtree/src/Slice.h"
#include <spdlog/spdlog.h>
#include <filesystem> 
#include <sstream>
#include <string>
#include <vector>
#include <csignal>
#include <sys/signalfd.h>
#include <unistd.h>

using namespace raft_rpc;

int main(int argc, char* argv[]) {
    if (argc < 4) {
        spdlog::error("Usage: {} [NodeID] [ListenPort] [Peer1_Port|Peer1_ID:Port] ...", argv[0]);
        return -1;
    }

    spdlog::set_level(spdlog::level::info);

    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &mask, nullptr) != 0) {
        spdlog::critical("[System] 设置 pthread_sigmask 失败！");
        return -1;
    }

    uint32_t my_id = static_cast<uint32_t>(std::stoi(argv[1]));
    uint16_t my_port = static_cast<uint16_t>(std::stoi(argv[2]));
    uint16_t client_service_port = my_port + 1000;

    std::string storage_kv_dir = "node_" + std::to_string(my_id) + "_storage/kv_data";
    std::filesystem::create_directories(storage_kv_dir);
    spdlog::info("[System] 磁盘存储盒子隔离空间准备就绪: {}", storage_kv_dir);

    EventLoop loop;
    
    int sig_fd = ::signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (sig_fd < 0) {
        spdlog::critical("[System] 创建 signalfd 失败！errno: {}", errno);
        return -1;
    }

    TcpServer internal_rpc_server(&loop, "127.0.0.1", my_port);
    RaftDispatcher dispatcher;
    TcpServer client_service_server(&loop, "127.0.0.1", client_service_port);

    std::vector<uint32_t> peer_ids;
    for (int i = 3; i < argc; ++i) {
        std::string arg = argv[i];
        uint32_t peer_id = 0;
        size_t pos = arg.find(':');
        if (pos != std::string::npos) {
            peer_id = static_cast<uint32_t>(std::stoi(arg.substr(0, pos)));
        } else {
            uint16_t peer_port = static_cast<uint16_t>(std::stoi(arg));
            peer_id = (peer_port >= 8881 && peer_port <= 8889) ? (peer_port - 8880) : peer_port;
        }

        if (peer_id != my_id) {
            peer_ids.push_back(peer_id);
        }
    }

    auto raft_node = std::make_shared<RaftNode>(&loop, my_id, peer_ids, &dispatcher);

    for (int i = 3; i < argc; ++i) {
        std::string arg = argv[i];
        uint32_t peer_id = 0;
        uint16_t peer_port = 0;

        size_t pos = arg.find(':');
        if (pos != std::string::npos) {
            peer_id = static_cast<uint32_t>(std::stoi(arg.substr(0, pos)));
            peer_port = static_cast<uint16_t>(std::stoi(arg.substr(pos + 1)));
        } else {
            peer_port = static_cast<uint16_t>(std::stoi(arg));
            peer_id = (peer_port >= 8881 && peer_port <= 8889) ? (peer_port - 8880) : peer_port;
        }

        if (peer_id != my_id) {
            raft_node->AddPeer(peer_id, "127.0.0.1", peer_port);
        }
    }

    dispatcher.SetPreVoteRequestCallback([raft_node](const std::shared_ptr<Connection>& conn, uint64_t req_id, const PreVoteArgs& args) {
        raft_node->HandlePreVote(conn, req_id, args);
    });
    dispatcher.SetPreVoteReplyCallback([raft_node](uint64_t req_id, const PreVoteReply& reply) {
        raft_node->HandlePreVoteReply(req_id, reply);
    });
    dispatcher.SetVoteRequestCallback([raft_node](const std::shared_ptr<Connection>& conn, uint64_t req_id, const RequestVoteArgs& args) {
        raft_node->HandleRequestVote(conn, req_id, args);
    });
    dispatcher.SetVoteReplyCallback([raft_node](uint64_t req_id, const RequestVoteReply& reply) {
        raft_node->HandleRequestVoteReply(req_id, reply);
    });
    dispatcher.SetHeartbeatRequestCallback([raft_node](const std::shared_ptr<Connection>& conn, uint64_t req_id, const AppendEntriesArgs& args) {
        raft_node->HandleAppendEntries(conn, req_id, args);
    });
    dispatcher.SetHeartbeatReplyCallback([raft_node](uint64_t req_id, const AppendEntriesReply& reply) {
        raft_node->HandleAppendEntriesReply(req_id, reply);
    });
    dispatcher.SetSnapshotRequestCallback([raft_node](const std::shared_ptr<Connection>& conn, uint64_t req_id, const InstallSnapshotArgs& args) {
        raft_node->HandleInstallSnapshot(conn, req_id, args);
    });
    dispatcher.SetSnapshotReplyCallback([raft_node](uint64_t req_id, const InstallSnapshotReply& reply) {
        raft_node->HandleInstallSnapshotReply(req_id, reply);
    });

    internal_rpc_server.SetMessageCallback([&dispatcher](const std::shared_ptr<Connection>& conn, Buffer* buf) {
        dispatcher.OnMessage(conn, buf);
    });

    client_service_server.SetMessageCallback([raft_node](const std::shared_ptr<Connection>& conn, Buffer* buf) {
        while (buf->ReadableBytes() >= raft_node::WireProtocol::kHeaderSize) {
            std::string_view raw_stream(buf->Peek(), buf->ReadableBytes());
            auto parsed_cmd = raft_node::WireProtocol::Parse(raw_stream);
            
            if (!parsed_cmd.has_value()) {
                auto header_res = raft_node::WireProtocol::ParseHeaderWithProbe(raw_stream);
                if (header_res.has_value() && header_res->second > 0) {
                    buf->Retrieve(header_res->second);
                    continue;
                }
                break; 
            }

            auto [header, key_view, value_view, skip] = *parsed_cmd;
            buf->Retrieve(skip + raft_node::WireProtocol::kHeaderSize + header.body_len);

            if (header.opcode == raft_node::Opcode::PUT_RAW) {
                bool accepted = raft_node->Propose(Slice(key_view), value_view, conn, header.req_id);
                if (!accepted) {
                    uint32_t leader_id = raft_node->GetLeaderId();
                    uint16_t leader_port = (leader_id != RaftCore::kNoLeader) ? (8880 + static_cast<uint16_t>(leader_id) + 1000) : 0;
                    
                    std::string reject_payload = "REJECT:" + std::to_string(leader_port);
                    std::string reject_bin = raft_node::WireProtocol::Serialize(
                        raft_node::Opcode::PUT_RAW, Slice(key_view), reject_payload, header.req_id
                    );
                    conn->Send(reject_bin);
                }
            } 
            else if (header.opcode == raft_node::Opcode::GET_RAW) {
                bool accepted = raft_node->ProposeRead(Slice(key_view), conn, header.req_id);
                if (!accepted) {
                    uint32_t leader_id = raft_node->GetLeaderId();
                    uint16_t leader_port = (leader_id != RaftCore::kNoLeader) ? (8880 + static_cast<uint16_t>(leader_id) + 1000) : 0;
                    
                    std::string reject_payload = "REJECT:" + std::to_string(leader_port);
                    std::string reject_bin = raft_node::WireProtocol::Serialize(
                        raft_node::Opcode::GET_RAW, Slice(key_view), reject_payload, header.req_id
                    );
                    conn->Send(reject_bin);
                }
            }
        }
    });

    auto sig_channel = std::make_unique<Channel>(&loop, sig_fd);
    sig_channel->SetReadCallback([&]() {
        struct signalfd_siginfo fdsi;
        ssize_t s = ::read(sig_fd, &fdsi, sizeof(fdsi));
        if (s == sizeof(fdsi)) {
            spdlog::warn("[System] 拦截到系统中断信号 (signo: {})，正式启动四阶段优雅停机...", fdsi.ssi_signo);
            
            sig_channel->DisableAll();
            sig_channel->Remove();

            client_service_server.Stop();

            raft_node->GracefulShutdown([&loop]() {
                spdlog::info("[System] 优雅停机全流程闭环，核心 EventLoop 安全退出。");
                loop.Quit();
            });
        }
    });
    sig_channel->EnableReading();

    internal_rpc_server.Start();
    client_service_server.Start();
    raft_node->Start();

    uint16_t http_port = 8080 + my_id;
    std::thread http_thread([raft_node, my_id, http_port]() {
        httplib::Server svr;
        svr.Get("/api/status", [raft_node](const httplib::Request&, httplib::Response& res) {
            auto status = raft_node->GetStatus();
            std::stringstream ss;
            ss << "{"
               << "\"node_id\":" << status.node_id << ","
               << "\"role\":\"" << status.role << "\","
               << "\"term\":" << status.term << ","
               << "\"leader_id\":" << (status.leader_id == RaftCore::kNoLeader ? -1 : (int)status.leader_id) << ","
               << "\"last_log_index\":" << status.last_log_index << ","
               << "\"commit_index\":" << status.commit_index << ","
               << "\"stabled_index\":" << status.stabled_index << ","
               << "\"last_applied\":" << status.last_applied
               << "}";
            res.set_header("Access-Control-Allow-Origin", "*");
            res.set_content(ss.str(), "application/json");
        });
        svr.listen("0.0.0.0", http_port);
    });
    http_thread.detach();

    spdlog::info("[System] 内部对账门面 [{}] 与 外部业务门面 [{}] 并网成功！", my_port, client_service_port);
    loop.Loop();

    ::close(sig_fd);
    spdlog::info("[System] 进程生命周期正常结束，退出码 0。");
    return 0;
}