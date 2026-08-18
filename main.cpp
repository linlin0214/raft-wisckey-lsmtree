#include "network/EventLoop.h"
#include "network/TcpServer.h"
#include "protocol/RaftDispatcher.h"
#include "raft/RaftNode.h"
#include "protocol/wire_protocol.h"  
#include "protocol/RaftRpc.h"
#include "lsmtree/third_party/httplib.h"
#include <spdlog/spdlog.h>
#include <filesystem> 
#include <sstream>
#include <string>
#include <vector>

using namespace raft_rpc;

int main(int argc, char* argv[]) {
    if (argc < 4) {
        spdlog::error("Usage: {} [NodeID] [ListenPort] [Peer1_Port|Peer1_ID:Port] ...", argv[0]);
        return -1;
    }

    // 设置日志级别为 info，保留关键生命周期节点变动，过滤日常心跳与发包追踪
    spdlog::set_level(spdlog::level::info);

    uint32_t my_id = static_cast<uint32_t>(std::stoi(argv[1]));
    uint16_t my_port = static_cast<uint16_t>(std::stoi(argv[2]));
    uint16_t client_service_port = my_port + 1000;

    std::string storage_kv_dir = "node_" + std::to_string(my_id) + "_storage/kv_data";
    std::filesystem::create_directories(storage_kv_dir);
    spdlog::info("[System] 磁盘存储盒子隔离空间准备就绪: {}", storage_kv_dir);

    EventLoop loop;
    
    TcpServer internal_rpc_server(&loop, "127.0.0.1", my_port);
    RaftDispatcher dispatcher;
    TcpServer client_service_server(&loop, "127.0.0.1", client_service_port);

    // 1. 解析 Peer ID 列表
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

    // 2. 实例化 Raft 共识节点
    auto raft_node = std::make_shared<RaftNode>(&loop, my_id, peer_ids, &dispatcher);

    // 3. 向 RaftNode 填充网络层 peers_ 路由映射表
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

    // 4. 注册分发器回调
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
                break; 
            }

            auto& header = std::get<0>(*parsed_cmd);
            auto& value_view = std::get<1>(*parsed_cmd);
            
            buf->Retrieve(raft_node::WireProtocol::kHeaderSize + header.val_len);

            if (header.opcode == raft_node::Opcode::PUT_RAW) {
                bool accepted = raft_node->Propose(header.key, value_view, conn);
                if (!accepted) {
                    uint32_t leader_id = raft_node->GetLeaderId();
                    uint16_t leader_port = (leader_id != RaftCore::kNoLeader) ? (static_cast<uint16_t>(leader_id) + 1000) : 0;
                    
                    std::string reject_payload = "REJECT:" + std::to_string(leader_port);
                    std::string reject_bin = raft_node::WireProtocol::Serialize(
                        raft_node::Opcode::PUT_RAW, header.key, reject_payload
                    );
                    conn->Send(reject_bin);
                }
            } 
            else if (header.opcode == raft_node::Opcode::GET_RAW) {
                bool accepted = raft_node->ProposeRead(header.key, conn);
                if (!accepted) {
                    uint32_t leader_id = raft_node->GetLeaderId();
                    uint16_t leader_port = (leader_id != RaftCore::kNoLeader) ? (static_cast<uint16_t>(leader_id) + 1000) : 0;
                    
                    std::string reject_payload = "REJECT:" + std::to_string(leader_port);
                    std::string reject_bin = raft_node::WireProtocol::Serialize(
                        raft_node::Opcode::GET_RAW, header.key, reject_payload
                    );
                    conn->Send(reject_bin);
                }
            }
        }
    });

    internal_rpc_server.Start();
    client_service_server.Start();
    raft_node->Start();

    
    uint16_t http_port = 8080 + my_id; // Node 1: 8081, Node 2: 8082, Node 3: 8083

    std::thread http_thread([raft_node, my_id, http_port]() {
        httplib::Server svr;

        // 1. 状态 JSON API 接口
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

        // 2. 嵌入式 Dashboard 单页面 HTML
        svr.Get("/", [](const httplib::Request&, httplib::Response& res) {
            const char* html = R"rawhtml(
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>Raft 分布式集群实时控制台</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 20px; }
            h1 { text-align: center; color: #38bdf8; font-size: 28px; margin-bottom: 25px; }
            .grid { display: flex; justify-content: center; gap: 20px; flex-wrap: wrap; }
            .card { background: #1e293b; border-radius: 12px; padding: 20px; width: 320px; box-shadow: 0 10px 15px -3px rgba(0,0,0,0.5); border: 2px solid #334155; transition: all 0.3s ease; }
            .card.leader { border-color: #22c55e; box-shadow: 0 0 20px rgba(34, 197, 94, 0.4); }
            .card.follower { border-color: #3b82f6; }
            .card.candidate { border-color: #f59e0b; }
            .card.offline { border-color: #ef4444; opacity: 0.6; }
            .badge { display: inline-block; padding: 4px 12px; border-radius: 9999px; font-weight: bold; font-size: 14px; text-transform: uppercase; margin-bottom: 15px; }
            .badge-leader { background: #22c55e; color: #000; }
            .badge-follower { background: #3b82f6; color: #fff; }
            .badge-candidate { background: #f59e0b; color: #000; }
            .badge-offline { background: #ef4444; color: #fff; }
            .row { display: flex; justify-content: space-between; margin: 8px 0; border-bottom: 1px solid #334155; padding-bottom: 4px; font-size: 14px; }
            .label { color: #94a3b8; }
            .val { font-weight: bold; color: #f1f5f9; font-family: monospace; font-size: 15px; }
        </style>
    </head>
    <body>
        <h1>⚡ Raft + LSM-Tree 分布式集群实时状态大盘 ⚡</h1>
        <div class="grid" id="cluster-view"></div>

        <script>
            const nodes = [
                { id: 1, port: 8081 },
                { id: 2, port: 8082 },
                { id: 3, port: 8083 }
            ];

            async function updateStatus() {
                const container = document.getElementById('cluster-view');
                container.innerHTML = '';

                for (const n of nodes) {
                    let data = null;
                    try {
                        const res = await fetch(`http://${window.location.hostname}:${n.port}/api/status`, { signal: AbortSignal.timeout(400) });
                        if (res.ok) data = await res.json();
                    } catch (e) {}

                    const card = document.createElement('div');
                    if (!data) {
                        card.className = 'card offline';
                        card.innerHTML = `
                            <h2>Node ${n.id}</h2>
                            <span class="badge badge-offline">Offline / Crash</span>
                            <div class="row"><span class="label">HTTP Port:</span><span class="val">${n.port}</span></div>
                        `;
                    } else {
                        const roleClass = data.role.toLowerCase();
                        card.className = `card ${roleClass}`;
                        card.innerHTML = `
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <h2>Node ${data.node_id}</h2>
                                <span class="badge badge-${roleClass}">${data.role}</span>
                            </div>
                            <div class="row"><span class="label">Current Term:</span><span class="val">${data.term}</span></div>
                            <div class="row"><span class="label">Leader ID:</span><span class="val">${data.leader_id === -1 ? 'None' : 'Node ' + data.leader_id}</span></div>
                            <div class="row"><span class="label">Last Log Index:</span><span class="val" style="color:#38bdf8;">${data.last_log_index}</span></div>
                            <div class="row"><span class="label">Commit Index:</span><span class="val" style="color:#4ade80;">${data.commit_index}</span></div>
                            <div class="row"><span class="label">WAL Stabled:</span><span class="val">${data.stabled_index}</span></div>
                            <div class="row"><span class="label">DB Applied:</span><span class="val">${data.last_applied}</span></div>
                        `;
                    }
                    container.appendChild(card);
                }
            }
            setInterval(updateStatus, 500);
            updateStatus();
        </script>
    </body>
    </html>
            )rawhtml";
            res.set_content(html, "text/html; charset=utf-8");
        });

        spdlog::info("[System] 嵌入式 Web 可视化看板启动就绪: http://127.0.0.1:{}", http_port);
        svr.listen("0.0.0.0", http_port);
    });
    http_thread.detach();
    spdlog::info("[System] 内部对账门面 [{}] 与 外部业务门面 [{}] 并网成功！", my_port, client_service_port);
    loop.Loop();

    return 0;
}