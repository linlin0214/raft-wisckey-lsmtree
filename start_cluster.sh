#!/bin/bash

BIN_PATH="./build_raft/raft_node"

if [ ! -f "$BIN_PATH" ]; then
    echo "❌ 错误：未找到编译产物 $BIN_PATH，请先执行 cmake 编译！"
    exit 1
fi

echo "========================================================================="
echo "🌊 纯自研 3 节点 Raft 分布式集群准备物理点火 (TOML 配置驱动)..."
echo "========================================================================="

# 启动各节点后台运行
$BIN_PATH --config conf/node_1.toml 2>&1 | sed "s/^/[Node-1] /" &
$BIN_PATH --config conf/node_2.toml 2>&1 | sed "s/^/[Node-2] /" &
$BIN_PATH --config conf/node_3.toml 2>&1 | sed "s/^/[Node-3] /" &

echo "🚀 全线节点已在后台完成 EINPROGRESS 注入！"
echo "💡 提示：按下 Ctrl + C 将触发四阶段优雅停机..."

trap 'echo -e "\n🏳️ 收到熔断信号，等待各节点完成四阶段落盘与优雅退出..."; wait; echo "✅ 所有节点已安全退出。"; exit 0' INT

wait