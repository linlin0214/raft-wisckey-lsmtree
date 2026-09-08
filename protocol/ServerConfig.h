#pragma once

#include "toml.hpp"
#include <string>
#include <vector>
#include <cstdint>
#include <stdexcept>
#include <filesystem>
#include <spdlog/spdlog.h>

namespace config {

struct PeerConfig {
    uint32_t id{0};
    std::string ip{"127.0.0.1"};
    uint16_t port{8881};
};

struct ServerConfig {
    // 1. 节点基础信息与存储空间
    uint32_t node_id{1};
    std::string storage_base_dir{"node_1_storage"};

    // 2. 网络监听与线程模型
    std::string listen_ip{"127.0.0.1"};
    uint16_t rpc_port{8881};
    uint16_t client_port{9881};
    uint16_t http_port{8081};
    int sub_reactor_threads{3};
    int client_idle_timeout_sec{60};

    // 3. 存储引擎参数 (WiscKey / LSM)
    size_t gc_rate_limit_bytes{20 * 1024 * 1024}; // 20 MB/s
    int gc_interval_sec{10};
    bool disable_wal{true};

    // 4. Raft 共识参数
    uint64_t snapshot_count_threshold{500000};
    uint64_t snapshot_interval_sec{300};

    // 5. 对端集群拓扑列表
    std::vector<PeerConfig> peers;

    static ServerConfig LoadFromFile(const std::string& config_path) {
        if (!std::filesystem::exists(config_path)) {
            throw std::runtime_error("配置文件不存在: " + config_path);
        }

        toml::table tbl;
        try {
            tbl = toml::parse_file(config_path);
        } catch (const toml::parse_error& err) {
            throw std::runtime_error("TOML 配置解析失败: " + std::string(err.description()));
        }

        ServerConfig cfg;

        // [node]
        cfg.node_id = static_cast<uint32_t>(tbl["node"]["id"].value_or(1));
        cfg.storage_base_dir = tbl["node"]["storage_dir"].value_or("node_" + std::to_string(cfg.node_id) + "_storage");

        // [network]
        cfg.listen_ip = tbl["network"]["listen_ip"].value_or("127.0.0.1");
        cfg.rpc_port = static_cast<uint16_t>(tbl["network"]["rpc_port"].value_or(8881));
        cfg.client_port = static_cast<uint16_t>(tbl["network"]["client_port"].value_or(cfg.rpc_port + 1000));
        cfg.http_port = static_cast<uint16_t>(tbl["network"]["http_port"].value_or(8080 + cfg.node_id));
        cfg.sub_reactor_threads = static_cast<int>(tbl["network"]["sub_reactor_threads"].value_or(3));
        cfg.client_idle_timeout_sec = static_cast<int>(tbl["network"]["client_idle_timeout_sec"].value_or(60));

        // [storage]
        int gc_mb = static_cast<int>(tbl["storage"]["gc_rate_limit_mb"].value_or(20));
        cfg.gc_rate_limit_bytes = static_cast<size_t>(gc_mb) * 1024 * 1024;
        cfg.gc_interval_sec = static_cast<int>(tbl["storage"]["gc_interval_sec"].value_or(10));
        cfg.disable_wal = tbl["storage"]["disable_wal"].value_or(true);

        // [raft]
        cfg.snapshot_count_threshold = tbl["raft"]["snapshot_count_threshold"].value_or(500000ULL);
        cfg.snapshot_interval_sec = tbl["raft"]["snapshot_interval_sec"].value_or(300ULL);

        // [[peers]]
        if (auto peers_arr = tbl["peers"].as_array()) {
            for (auto&& elem : *peers_arr) {
                if (auto peer_tbl = elem.as_table()) {
                    PeerConfig p;
                    p.id = static_cast<uint32_t>((*peer_tbl)["id"].value_or(0));
                    p.ip = (*peer_tbl)["ip"].value_or("127.0.0.1");
                    p.port = static_cast<uint16_t>((*peer_tbl)["port"].value_or(8880));
                    if (p.id != cfg.node_id) {
                        cfg.peers.push_back(p);
                    }
                }
            }
        }

        return cfg;
    }
};

} // namespace config